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
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.rpc.ExecutorRpcHandler;
import com.trs.pacifica.rpc.RpcContext;
import com.trs.pacifica.rpc.RpcHandler;
import com.trs.pacifica.rpc.RpcServer;
import com.trs.pacifica.rpc.node.Endpoint;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class GrpcServer implements RpcServer{
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServer.class);

    protected final Endpoint endpoint;

    private Server server;

    private final List<ServerInterceptor> serverInterceptors = new CopyOnWriteArrayList<>();
    private final MutableHandlerRegistry mutableHandlerRegistry = new MutableHandlerRegistry();

    private final MarshallerManager requestMarshallerManager;

    private final MarshallerManager responseMarshallerManager;


    private volatile boolean stopped = true;

    public GrpcServer(Endpoint endpoint, MarshallerManager requestMarshallerManager, MarshallerManager responseMarshallerManager) {
        this.endpoint = endpoint;
        this.requestMarshallerManager = requestMarshallerManager;
        this.responseMarshallerManager = responseMarshallerManager;
    }


    @Override
    public synchronized void init(Object option) throws PacificaException {
        this.registerDefaultInterceptor();
        this.addInterceptor(this.serverInterceptors);
    }

    @Override
    public synchronized void startup() throws PacificaException {
        if (isStopped()) {
            try {
                this.server = buildServer(this.endpoint);
                this.server.start();
                this.stopped = false;
                LOGGER.info("the rpc server is {} , port={}", this.server.getClass().getSimpleName(), this.server.getPort());
            } catch (IOException e) {
                throw new PacificaException(PacificaErrorCode.IO, "failed to startup.", e);
            }
        }
    }

    @Override
    public synchronized void shutdown() {
        if (!isStopped()) {
            this.stopped = true;
            shutdownAndAwaitTermination(this.server);
        }
    }

    public boolean isStopped() {
        return this.stopped;
    }

    protected Server buildServer(final Endpoint endpoint) {
        final int port = this.endpoint.getPort();
        final int maxInboundMessageSize = RPC_SERVER_MAX_INBOUND_MESSAGE_SIZE;
        return NettyServerBuilder.forPort(port)//
                .fallbackHandlerRegistry(this.mutableHandlerRegistry)//
                .maxInboundMessageSize(maxInboundMessageSize)//
                .directExecutor()//
                .build();
    }


    @Override
    public void registerRpcHandler(RpcHandler rpcHandler) {
        final String requestClzName = rpcHandler.interest();
        Objects.requireNonNull(requestClzName, "rpcHandler.interest()");

        final ExecutorRpcHandler executorRpcHandler = ExecutorRpcHandler.wrap(rpcHandler);
        //method
        final MethodDescriptor<Message, Message> method = MethodDescriptor //
                .<Message, Message>newBuilder() //
                .setType(MethodDescriptor.MethodType.UNARY) //
                .setFullMethodName(getFullMethodName(requestClzName)) //
                .setRequestMarshaller(getRequestMarshaller(requestClzName)) //
                .setResponseMarshaller(getResponseMarshaller(requestClzName)) //
                .build();
        //handler
        final ServerCallHandler<Message, Message> handler = ServerCalls.asyncUnaryCall(
                (request, responseObserver) -> {
                    final RpcContext<Message> rpcContext = getRpcContext(responseObserver);
                    try {
                        executorRpcHandler.handleRequest(rpcContext, request);
                    } catch (Throwable e) {
                        LOGGER.error("failed to handle request={}.", request.getClass().getSimpleName(), e);
                        Status status = Status//
                                .INTERNAL//
                                .augmentDescription(e.getMessage())//
                                .withCause(e);
                        responseObserver.onError(status.asException());
                    }
                }
        );
        //server service
        final ServerServiceDefinition serviceDefinition = ServerServiceDefinition //
                .builder(requestClzName) //
                .addMethod(method, handler) //
                .build();
        //intercept
        final ServerServiceDefinition serviceDefinitionLine = ServerInterceptors.intercept(serviceDefinition, this.serverInterceptors);
        this.mutableHandlerRegistry.addService(serviceDefinitionLine);

    }

    private static RpcContext<Message> getRpcContext(StreamObserver<Message> responseObserver) {
        final SocketAddress remoteAddress = RemoteAddressInterceptor.getRemoteAddress();
        final RpcContext<Message> rpcContext = new RpcContext<>() {
            @Override
            public void sendResponse(Message response) {
                try {
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Throwable e) {
                    LOGGER.warn("failed to send response, may be repeated.", e);
                }
            }
            @Override
            public String getRemoteAddress() {
                return remoteAddress != null ? remoteAddress.toString() : null;
            }
        };
        return rpcContext;
    }


    protected void registerDefaultInterceptor() {
        this.serverInterceptors.add(new RemoteAddressInterceptor());
    }

    protected void addInterceptor(List<ServerInterceptor> interceptorContainer) {

    }

    public void extendMarshaller(Message request, Message response) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(response, "response");
        final String requestClzName = request.getClass().getName();
        this.requestMarshallerManager.registerMarshaller(requestClzName, request);
        this.responseMarshallerManager.registerMarshaller(requestClzName, response);
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

    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 1000;

    public static boolean shutdownAndAwaitTermination(final Server server) {
        return shutdownAndAwaitTermination(server, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    public static boolean shutdownAndAwaitTermination(final Server server, final long timeoutMillis) {
        if (server == null) {
            return true;
        }
        // disable new tasks from being submitted
        server.shutdown();
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        final long phaseOne = timeoutMillis / 5;
        try {
            // wait a while for existing tasks to terminate
            if (server.awaitTermination(phaseOne, unit)) {
                return true;
            }
            server.shutdownNow();
            // wait a while for tasks to respond to being cancelled
            if (server.awaitTermination(timeoutMillis - phaseOne, unit)) {
                return true;
            }
            LOGGER.warn("Fail to shutdown grpc server: {}.", server);
        } catch (final InterruptedException e) {
            // (Re-)cancel if current thread also interrupted
            server.shutdownNow();
            // preserve interrupt status
            Thread.currentThread().interrupt();
        }
        return false;
    }

}
