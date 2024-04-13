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

    public static String toLogString(final RpcCommon.ReplicaId replicaId) {
        return replicaId.getGroupName() + "_" + replicaId.getNodeId();
    }

    public static String toLogString(final RpcRequest.InstallSnapshotRequest request) {
        StringBuilder logInfo = new StringBuilder();
        logInfo.append("primary_id=").append(toLogString(request.getPrimaryId())).append(",");
        logInfo.append("target_id=").append(toLogString(request.getTargetId())).append(",");
        logInfo.append("snapshot_log_index=").append(request.getSnapshotLogIndex()).append(",");
        logInfo.append("snapshot_log_term=").append(request.getSnapshotLogTerm()).append(",");
        logInfo.append("reader_id=").append(request.getReaderId()).append(",");
        return logInfo.toString();
    }
}
