/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.trs.pacifica.rpc.impl.grpc;

import com.google.protobuf.Message;
import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.rpc.client.ExecutorInvokeCallback;
import com.trs.pacifica.rpc.client.InvokeCallback;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.error.ConnectionException;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.util.SystemPropertyUtil;
import com.trs.pacifica.util.ThrowsUtil;
import io.grpc.*;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class GrpcClient implements RpcClient, LifeCycle<GrpcClient.Option> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcClient.class);
    private static final int RESET_CONN_THRESHOLD = SystemPropertyUtil.getInt(
            "jpacifica.grpc.max.conn.failures.to_reset", 2);

    private final Map<Endpoint, ManagedChannel> channelContainer = new ConcurrentHashMap<>();
    private final Map<Endpoint, AtomicInteger> transientFailures = new ConcurrentHashMap<>();
    private final MarshallerManager requestMarshallerManager;
    private final MarshallerManager responseMarshallerManager;
    private volatile boolean stopped = true;

    private Option option = null;

    public GrpcClient(MarshallerManager requestMarshallerManager, MarshallerManager responseMarshallerManager) {
        this.requestMarshallerManager = requestMarshallerManager;
        this.responseMarshallerManager = responseMarshallerManager;
    }


    @Override
    public synchronized void init(Option option) throws PacificaException {
        this.option = option;
    }

    @Override
    public synchronized void startup() throws PacificaException {
        if (isStopped()) {

            this.stopped = false;
        }
    }

    @Override
    public synchronized void shutdown() throws PacificaException {
        if (!isStopped()) {
            this.stopped = true;
            closeAllChannel();
            this.transientFailures.clear();
        }
    }

    public boolean isStopped() {
        return this.stopped;
    }

    public void ensureOpen() {
        if (isStopped()) {
            throw new AlreadyClosedException(String.format("%s is stopped.", this.getClass().getSimpleName()));
        }
    }

    @Override
    public boolean checkConnection(Endpoint endpoint, boolean createIfAbsent) {
        Objects.requireNonNull(endpoint, "endpoint");
        return checkChannel(endpoint, createIfAbsent);
    }

    @Override
    public void invokeAsync(Endpoint endpoint, Object request, InvokeCallback callback, long timeoutMs) {
        ensureOpen();
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(request, "request");
        final ExecutorInvokeCallback invokeCallback = ExecutorInvokeCallback.wrap(callback);
        final ManagedChannel channel = getChannel(endpoint);
        if (channel == null) {
            invokeCallback.complete(null, new ConnectionException(String.format("The attempt to connect to %s failed", endpoint)));
            return;
        }
        if (timeoutMs < 0) {
            timeoutMs = Integer.MAX_VALUE;
        }
        // client option
        final CallOptions callOpts = CallOptions.DEFAULT//
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)//
                .withMaxInboundMessageSize(this.option.getMaxInboundMessageSize())//
                .withMaxOutboundMessageSize(this.option.getMaxOutboundMessageSize());//

        // method
        final MethodDescriptor<Message, Message> method = getCallMethod(request);

        // send request
        ClientCalls.asyncUnaryCall(channel.newCall(method, callOpts), (Message) request, new StreamObserver<Message>() {

            @Override
            public void onNext(final Message value) {
                invokeCallback.complete(value, null);
            }

            @Override
            public void onError(final Throwable throwable) {
                invokeCallback.complete(null, throwable);
            }

            @Override
            public void onCompleted() {
                // nothing
            }
        });
    }

    @Override
    public Object invokeSync(Endpoint endpoint, Object request, long timeoutMs) throws PacificaException {
        final CompletableFuture<Object> future = new CompletableFuture<>();
        invokeAsync(endpoint, request, (result, err) -> {
            if (err == null) {
                future.complete(result);
            } else {
                future.completeExceptionally(err);
            }
        }, timeoutMs);
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            future.cancel(true);
            throw new PacificaException(PacificaErrorCode.TIMEOUT, "rpc request timeout", e);
        } catch (final ExecutionException t) {
            future.cancel(true);
            final Throwable cause = ThrowsUtil.getCause(t);
            if (cause == null) {
                throw new PacificaException(PacificaErrorCode.UNDEFINED, "execution exception", t);
            } else if (cause instanceof PacificaException) {
                throw (PacificaException) cause;
            } else {
                throw new PacificaException(PacificaErrorCode.UNDEFINED, cause.getMessage(), cause);
            }
        } catch (Throwable e) {
            throw new PacificaException(PacificaErrorCode.UNDEFINED, e.getMessage(), e);
        }
    }

    private MethodDescriptor<Message, Message> getCallMethod(final Object request) {
        Objects.requireNonNull(request, "request");
        final String requestClzName = request.getClass().getName();
        return MethodDescriptor //
                .<Message, Message>newBuilder() //
                .setType(MethodDescriptor.MethodType.UNARY) //
                .setFullMethodName(getFullMethodName(requestClzName)) //
                .setRequestMarshaller(getRequestMarshaller(requestClzName)) //
                .setResponseMarshaller(getResponseMarshaller(requestClzName)) //
                .build();
    }


    private boolean checkChannel(final Endpoint endpoint, final boolean createIfAbsent) {
        final ManagedChannel ch = getChannel(endpoint, createIfAbsent);
        if (ch == null) {
            return false;
        }
        return checkConnectivity(endpoint, ch);
    }

    private boolean checkConnectivity(final Endpoint endpoint, final ManagedChannel channel) {
        final ConnectivityState st = channel.getState(false);
        if (st != ConnectivityState.TRANSIENT_FAILURE && st != ConnectivityState.SHUTDOWN) {
            return true;
        }
        final int c = incConnFailuresCount(endpoint);
        if (c < RESET_CONN_THRESHOLD) {
            if (c == RESET_CONN_THRESHOLD - 1) {
                // For sub-channels that are in TRANSIENT_FAILURE state, short-circuit the backoff timer and make
                // them reconnect immediately. May also attempt to invoke NameResolver#refresh
                channel.resetConnectBackoff();
            }
            return true;
        }
        clearConnFailuresCount(endpoint);
        final ManagedChannel removedCh = removeChannel(endpoint);
        if (removedCh == null) {
            // The channel has been removed and closed by another
            return false;
        }

        LOGGER.warn("Channel[{}] in [INACTIVE] state {} times, it has been removed from the pool.", endpoint, c);

        if (removedCh != channel) {
            // Now that it's removed, close it
            ManagedChannelHelper.shutdownAndAwaitTermination(removedCh, 100);
        }
        ManagedChannelHelper.shutdownAndAwaitTermination(channel, 100);
        return false;

    }

    private ManagedChannel getChannel(final Endpoint endpoint) {
        final ManagedChannel channel = getChannel(endpoint, true);
        if (checkConnectivity(endpoint, channel)) {
            return channel;
        }
        return null;
    }

    private ManagedChannel getChannel(final Endpoint endpoint, final boolean createIfAbsent) {
        if (createIfAbsent) {
            return this.channelContainer.computeIfAbsent(endpoint, this::createChannel);
        } else {
            return this.channelContainer.get(endpoint);
        }
    }

    private ManagedChannel createChannel(final Endpoint endpoint) {
        ensureOpen();
        final ManagedChannel ch = ManagedChannelBuilder//
                .forAddress(endpoint.getIp(), endpoint.getPort()) //
                .usePlaintext() //
                .directExecutor() //
                .maxInboundMessageSize(this.option.getMaxInboundMessageSize()) //
                .build();
        LOGGER.info("Creating new channel to: {}.", endpoint);
        return ch;
    }


    private ManagedChannel removeChannel(final Endpoint endpoint) {
        return this.channelContainer.remove(endpoint);
    }

    private void removeAndCloseChannel(final Endpoint endpoint) {
        final ManagedChannel channel = removeChannel(endpoint);
        if (channel != null) {
            LOGGER.info("Closing the connection {} to {}", channel, endpoint);
            ManagedChannelHelper.shutdownAndAwaitTermination(channel, 1000);
        }
    }

    private int incConnFailuresCount(final Endpoint endpoint) {
        return this.transientFailures.computeIfAbsent(endpoint, ep -> new AtomicInteger()).incrementAndGet();
    }

    private void clearConnFailuresCount(final Endpoint endpoint) {
        this.transientFailures.remove(endpoint);
    }

    private void closeAllChannel() {
        Set<Endpoint> allEndpoint = new HashSet<>();
        allEndpoint.addAll(this.channelContainer.keySet());
        allEndpoint.forEach(endpoint -> {
            removeAndCloseChannel(endpoint);
        });
    }

    protected String getFullMethodName(final String requestClzName) {
        return GrpcUtil.getFullMethodName(requestClzName);
    }

    protected MethodDescriptor.Marshaller<Message> getRequestMarshaller(String requestClzName) {
        return this.requestMarshallerManager.getMarshaller(requestClzName);
    }

    protected MethodDescriptor.Marshaller<Message> getResponseMarshaller(String requestClzName) {
        return this.responseMarshallerManager.getMarshaller(requestClzName);
    }

    public static class Option {

        public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 32 * 1024 * 1024;

        /**
         *
         */
        private int maxInboundMessageSize = DEFAULT_MAX_INBOUND_MESSAGE_SIZE;

        /**
         *
         */
        private int maxOutboundMessageSize = DEFAULT_MAX_INBOUND_MESSAGE_SIZE;

        public int getMaxInboundMessageSize() {
            return maxInboundMessageSize;
        }

        public void setMaxInboundMessageSize(int maxInboundMessageSize) {
            this.maxInboundMessageSize = maxInboundMessageSize;
        }

        public int getMaxOutboundMessageSize() {
            return maxOutboundMessageSize;
        }

        public void setMaxOutboundMessageSize(int maxOutboundMessageSize) {
            this.maxOutboundMessageSize = maxOutboundMessageSize;
        }
    }

}
