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

import com.google.protobuf.Message;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.rpc.node.EndpointFactory;

/**
 *
 */
public class DefaultPacificaClient extends BaseReplicaClient implements PacificaClient {

    private final EndpointFactory endpointFactory;

    public DefaultPacificaClient(RpcClient rpcClient, EndpointFactory endpointFactory) {
        super(rpcClient);
        this.endpointFactory = endpointFactory;
    }

    @Override
    public Message appendLogEntries(RpcRequest.AppendEntriesRequest request, RpcRequestFinished<RpcRequest.AppendEntriesResponse> callback, int timeoutMs) {
        return null;
    }

    @Override
    public Message installSnapshot(RpcRequest.InstallSnapshotRequest request, RpcRequestFinished<RpcRequest.InstallSnapshotResponse> callback, int timeoutMs) {
        return null;
    }

    @Override
    public Message recoverReplica(RpcRequest.ReplicaRecoverRequest request, RpcRequestFinished<RpcRequest.ReplicaRecoverResponse> callback, int timeoutMs) {
        return null;
    }

    @Override
    public Message getFile(RpcRequest.GetFileRequest request, RpcRequestFinished<RpcRequest.GetFileResponse> callback, long timeoutMs) {
        return null;
    }

    @Override
    protected Endpoint getEndpoint(ReplicaId targetReplicaId) {
        final String nodeId = targetReplicaId.getNodeId();
        return  endpointFactory.getEndpoint(nodeId);
    }
}
