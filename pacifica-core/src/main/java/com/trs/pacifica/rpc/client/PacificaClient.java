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

package com.trs.pacifica.rpc.client;

import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.util.SystemPropertyUtil;

import java.util.concurrent.Future;

public interface PacificaClient extends ReplicaClient {

    public static final int DEFAULT_TIMEOUT_MS = SystemPropertyUtil.getInt("pacifica.client.rpc.client.timeout.ms", 60 * 1000);

    /**
     * @param request
     * @param callback
     * @return
     */
    default Future<RpcRequest.AppendEntriesResponse> appendLogEntries(RpcRequest.AppendEntriesRequest request, RpcRequestFinished<RpcRequest.AppendEntriesResponse> callback) {
        return appendLogEntries(request, callback, DEFAULT_TIMEOUT_MS);
    }

    Future<RpcRequest.AppendEntriesResponse> appendLogEntries(RpcRequest.AppendEntriesRequest request, RpcRequestFinished<RpcRequest.AppendEntriesResponse> callback, int timeoutMs);


    default Future<RpcRequest.InstallSnapshotResponse> installSnapshot(RpcRequest.InstallSnapshotRequest request, RpcRequestFinished<RpcRequest.InstallSnapshotResponse> callback) {
        return installSnapshot(request, callback, DEFAULT_TIMEOUT_MS);
    }

    Future<RpcRequest.InstallSnapshotResponse> installSnapshot(RpcRequest.InstallSnapshotRequest request, RpcRequestFinished<RpcRequest.InstallSnapshotResponse> callback, int timeoutMs);

    Future<RpcRequest.ReplicaRecoverResponse> recoverReplica(RpcRequest.ReplicaRecoverRequest request, RpcRequestFinished<RpcRequest.ReplicaRecoverResponse> callback, int timeoutMs);

    default Future<RpcRequest.ReplicaRecoverResponse> recoverReplica(RpcRequest.ReplicaRecoverRequest request, RpcRequestFinished<RpcRequest.ReplicaRecoverResponse> callback) {
        return recoverReplica(request, callback, DEFAULT_TIMEOUT_MS);
    }

    Future<RpcRequest.GetFileResponse> getFile(RpcRequest.GetFileRequest request, RpcRequestFinished<RpcRequest.GetFileResponse> callback, final int timeoutMs);


    default Future<RpcRequest.GetFileResponse> getFile(RpcRequest.GetFileRequest request, RpcRequestFinished<RpcRequest.GetFileResponse> callback) {
        return getFile(request, callback, DEFAULT_TIMEOUT_MS);
    }

}
