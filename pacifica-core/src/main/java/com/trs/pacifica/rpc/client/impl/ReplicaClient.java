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
import com.trs.pacifica.error.NotFoundEndpointException;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcResponseCallback;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.rpc.node.NodeManager;

import java.util.Objects;

/**
 *
 */
public class ReplicaClient implements PacificaClient {

    private final RpcClient rpcClient;

    private final NodeManager nodeManager;

    public ReplicaClient(RpcClient rpcClient, NodeManager nodeManager) {
        this.rpcClient = rpcClient;
        this.nodeManager = nodeManager;
    }


    @Override
    public boolean connect(ReplicaId targetReplicaId) {
        return true;
    }

    @Override
    public boolean disconnect(ReplicaId targetReplicaId) {
        return true;
    }

    @Override
    public boolean checkConnection(ReplicaId targetReplicaId, boolean createIfAbsent) {
        Objects.requireNonNull(targetReplicaId, "targetReplicaId");
        final String nodeId = targetReplicaId.getNodeId();
        Objects.requireNonNull(nodeId, "targetReplicaId.nodeId");
        final Endpoint endpoint = nodeManager.getEndpoint(targetReplicaId.getNodeId());
        if (endpoint == null) {
            throw new NotFoundEndpointException(String.format("not found address of nodeId=%s", nodeId));
        }
        return rpcClient.checkConnection(endpoint, createIfAbsent);
    }

    @Override
    public Message appendLogEntries(RpcRequest.AppendEntriesRequest request, RpcResponseCallback<RpcRequest.AppendEntriesResponse> callback, int timeoutMs) {
        return null;
    }

    @Override
    public Message installSnapshot(RpcRequest.InstallSnapshotRequest request, RpcResponseCallback<RpcRequest.InstallSnapshotResponse> callback, int timeoutMs) {
        return null;
    }

    @Override
    public Message recoverReplica(RpcRequest.ReplicaRecoverRequest request, RpcResponseCallback<RpcRequest.ReplicaRecoverResponse> callback, int timeoutMs) {
        return null;
    }

    @Override
    public Message getFile(RpcRequest.GetFileRequest request, RpcResponseCallback<RpcRequest.GetFileResponse> callback, long timeoutMs) {
        return null;
    }
}
