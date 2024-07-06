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

package com.trs.pacifica.util;

import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.proto.RpcRequest;

public class RpcLogUtil {
    private RpcLogUtil() {

    }

    public static String toLogInfo(final RpcCommon.ReplicaId replicaId) {
        return replicaId.getGroupName() + "_" + replicaId.getNodeId();
    }

    public static String toLogInfo(final RpcRequest.InstallSnapshotRequest request) {
        StringBuilder logInfo = new StringBuilder();
        logInfo.append("primary_id=").append(toLogInfo(request.getPrimaryId())).append(",");
        logInfo.append("target_id=").append(toLogInfo(request.getTargetId())).append(",");
        logInfo.append("snapshot_log_index=").append(request.getSnapshotLogIndex()).append(",");
        logInfo.append("snapshot_log_term=").append(request.getSnapshotLogTerm()).append(",");
        logInfo.append("reader_id=").append(request.getReaderId()).append(",");
        return logInfo.toString();
    }

    public static String toLogInfo(final RpcRequest.InstallSnapshotResponse response) {
        StringBuilder logInfo = new StringBuilder();
        logInfo.append("success=").append(response.getSuccess()).append(",");
        logInfo.append("term=").append(response.getTerm()).append(",");
        logInfo.append("version=").append(response.getVersion());
        return logInfo.toString();
    }

    public static String toLogInfo(RpcRequest.AppendEntriesRequest request) {
        StringBuilder infoBuilder = new StringBuilder("AppendEntriesRequest[");
        infoBuilder.append("primary_id=").append(RpcUtil.toReplicaId(request.getPrimaryId())).append(",");
        infoBuilder.append("target_id=").append(RpcUtil.toReplicaId(request.getTargetId())).append(",");
        infoBuilder.append("prev_log_index=").append(request.getPrevLogIndex()).append(",");
        infoBuilder.append("prev_log_term=").append(request.getPrevLogTerm()).append(",");
        infoBuilder.append("commit_point=").append(request.getCommitPoint()).append(",");
        infoBuilder.append("term=").append(request.getTerm()).append(",");
        infoBuilder.append("version=").append(request.getVersion()).append(",");
        infoBuilder.append("log_entry_count=").append(request.getLogMetaCount());
        return infoBuilder.append("]").toString();
    }

    public static String toLogInfo(RpcRequest.AppendEntriesResponse response) {
        StringBuilder infoBuilder = new StringBuilder("AppendEntriesResponse[");
        infoBuilder.append("success=").append(response.getSuccess()).append(",");
        infoBuilder.append("term=").append(response.getTerm()).append(",");
        infoBuilder.append("version=").append(response.getVersion()).append(",");
        infoBuilder.append("commit_point=").append(response.getCommitPoint()).append(",");
        infoBuilder.append("last_log_index=").append(response.getLastLogIndex());
        return infoBuilder.append("]").toString();
    }
}
