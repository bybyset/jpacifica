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

package com.trs.pacifica.rpc.client.impl;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.error.NotFoundEndpointException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.rpc.client.InvokeCallback;
import com.trs.pacifica.rpc.client.ReplicaClient;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public abstract class BaseReplicaClient implements ReplicaClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseReplicaClient.class);

    protected final RpcClient rpcClient;

    public BaseReplicaClient(RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public boolean connect(ReplicaId targetReplicaId) {
        return checkConnection(targetReplicaId, true);
    }

    @Override
    public boolean checkConnection(ReplicaId targetReplicaId, boolean createIfAbsent) {
        Objects.requireNonNull(targetReplicaId, "targetReplicaId");
        //1. find endpoint
        final Endpoint endpoint = getEndpointOrThrow(targetReplicaId);
        //2. check connection to endpoint
        if (!this.rpcClient.checkConnection(endpoint, createIfAbsent)) {
            return false;
        }
        //3. send PingRequest
        return pingReplica(endpoint, targetReplicaId);
    }

    protected boolean pingReplica(Endpoint endpoint, final ReplicaId targetReplicaId) {
        final RpcRequest.PingReplicaRequest pingReplicaRequest = RpcRequest.PingReplicaRequest//
                .newBuilder()//
                .setTargetId(RpcUtil.protoReplicaId(targetReplicaId))//
                .build();
        try {
            this.rpcClient.invokeSync(endpoint, pingReplicaRequest, 60 * 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    /**
     * get endpoint of target replica, or throw exception if not found
     *
     * @param targetReplicaId
     * @return
     * @throws NotFoundEndpointException
     */
    protected Endpoint getEndpointOrThrow(final ReplicaId targetReplicaId) throws NotFoundEndpointException {
        Objects.requireNonNull(targetReplicaId, "targetReplicaId");
        final Endpoint endpoint = getEndpoint(targetReplicaId);
        if (endpoint == null) {
            throw new NotFoundEndpointException(String.format("not found address of replica=%s", targetReplicaId));
        }
        return endpoint;
    }


    @Override
    public <T extends Message> Future<T> sendRequest(final Endpoint endpoint, final Message request, final RpcRequestFinished<T> callback, final int timeoutMs, final Executor callbackExecutor) {
        final FutureImpl<T> future = new FutureImpl<>();
        try {
            this.rpcClient.invokeAsync(endpoint, request, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {
                    if (future.isCancelled()) {
                        ThreadUtil.runCallback(callback, Finished.failure(new CancellationException("it's been canceled.")));
                        return;
                    }
                    if (err != null) {
                        // error
                        ThreadUtil.runCallback(callback, Finished.failure(err));
                    } else {
                        if (result != null) {
                            if (result instanceof Message) {
                                final Message response = (Message) result;
                                try {
                                    checkResponse(response);
                                    callback.setRpcResponse((T)response);
                                    ThreadUtil.runCallback(callback, Finished.success());
                                } catch (PacificaException e) {
                                    ThreadUtil.runCallback(callback, Finished.failure(e));
                                } catch (Throwable e) {
                                    LOGGER.warn("Unexpected exception.", e);
                                    ThreadUtil.runCallback(callback, Finished.failure(e));
                                }
                            } else {
                                ThreadUtil.runCallback(callback, Finished.failure(new PacificaException(PacificaErrorCode.UNDEFINED, "Invalid Response type")));
                            }
                        } else {
                            ThreadUtil.runCallback(callback, Finished.success());
                        }
                    }
                }

                @Override
                public Executor executor() {
                    if (callbackExecutor == null) {
                        return InvokeCallback.super.executor();
                    }
                    return callbackExecutor;
                }

            }, timeoutMs);
        } catch (Throwable e) {
            ThreadUtil.runCallback(callback, Finished.failure(e));
        }
        return future;
    }

    private void checkResponse(Message response) throws PacificaException {
        final Descriptors.FieldDescriptor errorFd = RpcUtil.findErrorFieldDescriptor(response);
        if (errorFd != null && response.hasField(errorFd)) {
            final RpcRequest.ErrorResponse errorResponse = (RpcRequest.ErrorResponse) response.getField(errorFd);
            throw RpcUtil.toPacificaException(errorResponse);
        }
    }

    /**
     * get endpoint of target replica
     *
     * @param targetReplicaId
     * @return null if not found
     */
    protected abstract Endpoint getEndpoint(final ReplicaId targetReplicaId);


}
