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

package com.trs.pacifica;

import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.proto.RpcRequest.AppendEntriesResponse;
import com.trs.pacifica.proto.RpcRequest.AppendEntriesRequest;
import com.trs.pacifica.proto.RpcRequest.ReplicaRecoverRequest;
import com.trs.pacifica.proto.RpcRequest.ReplicaRecoverResponse;
import com.trs.pacifica.proto.RpcRequest.InstallSnapshotRequest;
import com.trs.pacifica.proto.RpcRequest.InstallSnapshotResponse;
import com.trs.pacifica.proto.RpcRequest.GetFileRequest;
import com.trs.pacifica.proto.RpcRequest.GetFileResponse;
import com.trs.pacifica.rpc.RpcResponseCallback;

public interface ReplicaService {


    /**
     * @param request
     * @param callback
     * @return
     * @throws PacificaException
     */
    public AppendEntriesResponse handleAppendLogEntryRequest(AppendEntriesRequest request, RpcResponseCallback<AppendEntriesResponse> callback) throws PacificaException;


    public ReplicaRecoverResponse handleReplicaRecoverRequest(ReplicaRecoverRequest request, RpcResponseCallback<ReplicaRecoverResponse> callback) throws PacificaException;


    public InstallSnapshotResponse handleInstallSnapshotRequest(InstallSnapshotRequest request, RpcResponseCallback<InstallSnapshotResponse> callback) throws PacificaException;

    public GetFileResponse handleGetFileRequest(GetFileRequest request, RpcResponseCallback<GetFileResponse> callback) throws PacificaException;

}
