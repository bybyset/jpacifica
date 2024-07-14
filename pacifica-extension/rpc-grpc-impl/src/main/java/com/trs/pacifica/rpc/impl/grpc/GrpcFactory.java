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
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcFactory;
import com.trs.pacifica.rpc.RpcServer;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.spi.SPI;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@SPI
public class GrpcFactory implements RpcFactory {

    public static final String FIXED_METHOD_NAME = "_call";
    private final Map<String, ReqRepMessage> messageContainer = new ConcurrentHashMap<>();
    private final MarshallerManager requestMarshallerManager = new MarshallerManager() {
        @Override
        public MethodDescriptor.Marshaller<Message> getMarshaller(String requestClzName) {
            final ReqRepMessage m = messageContainer.get(requestClzName);
            if (m != null && m.requestMessage != null) {
                return ProtoUtils.marshaller(m.requestMessage);
            }
            return null;
        }

        @Override
        public void registerMarshaller(String requestClzName, Message message) {
            Objects.requireNonNull(requestClzName, "requestClzName");
            Objects.requireNonNull(message, "message");
            ReqRepMessage reqRepMessage = messageContainer.computeIfAbsent(requestClzName, (key) -> {
                return new ReqRepMessage();
            });
            reqRepMessage.setRequestMessage(message);
        }
    };
    private final MarshallerManager responseMarshallerManager = new MarshallerManager() {
        @Override
        public MethodDescriptor.Marshaller<Message> getMarshaller(String requestClzName) {
            final ReqRepMessage m = messageContainer.get(requestClzName);
            if (m != null && m.responseMessage != null) {
                return ProtoUtils.marshaller(m.responseMessage);
            }
            return null;
        }

        @Override
        public void registerMarshaller(String requestClzName, Message message) {
            Objects.requireNonNull(requestClzName, "requestClzName");
            Objects.requireNonNull(message, "message");
            ReqRepMessage reqRepMessage = messageContainer.putIfAbsent(requestClzName, new ReqRepMessage());
            reqRepMessage.setResponseMessage(message);
        }
    };


    public GrpcFactory(){
        registerAll();
    }


    @Override
    public RpcClient createRpcClient() {
        return new GrpcClient(requestMarshallerManager, responseMarshallerManager);
    }

    @Override
    public RpcServer createRpcServer(final Endpoint endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        return creteNettyGrpcServer(endpoint);
    }

    protected RpcServer creteNettyGrpcServer(final Endpoint endpoint) {
        return new GrpcServer(endpoint, requestMarshallerManager, responseMarshallerManager);
    }


    public void register(Message request, Message response) {
        final String requestClzName = request.getClass().getName();
        this.messageContainer.put(requestClzName, new ReqRepMessage(request, response));
    }

    private void registerAll() {
        this.register(RpcRequest.AppendEntriesRequest.getDefaultInstance(), RpcRequest.AppendEntriesResponse.getDefaultInstance());
        this.register(RpcRequest.ReplicaRecoverRequest.getDefaultInstance(), RpcRequest.ReplicaRecoverResponse.getDefaultInstance());
        this.register(RpcRequest.InstallSnapshotRequest.getDefaultInstance(), RpcRequest.InstallSnapshotResponse.getDefaultInstance());
        this.register(RpcRequest.GetFileRequest.getDefaultInstance(), RpcRequest.GetFileResponse.getDefaultInstance());
        this.register(RpcRequest.PingReplicaRequest.getDefaultInstance(), RpcRequest.PingReplicaResponse.getDefaultInstance());
    }


    static class ReqRepMessage {
        private Message requestMessage;

        private Message responseMessage;

        ReqRepMessage(Message requestMessage, Message responseMessage) {
            this.requestMessage = requestMessage;
            this.responseMessage = responseMessage;
        }

        ReqRepMessage() {
        }

        public Message getRequestMessage() {
            return requestMessage;
        }

        public void setRequestMessage(Message requestMessage) {
            this.requestMessage = requestMessage;
        }

        public Message getResponseMessage() {
            return responseMessage;
        }

        public void setResponseMessage(Message responseMessage) {
            this.responseMessage = responseMessage;
        }
    }


}
