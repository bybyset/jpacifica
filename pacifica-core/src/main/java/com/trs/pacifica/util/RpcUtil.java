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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.proto.RpcRequest;

import java.nio.ByteBuffer;
import java.util.*;

public class RpcUtil {

    private RpcUtil() {

    }

    public static final String ERROR_FIELD_NAME = "error";

    public static Descriptors.FieldDescriptor findErrorFieldDescriptor(final Message message) {
        return message //
                .getDescriptorForType() //
                .findFieldByName(ERROR_FIELD_NAME);
    }

    public static ReplicaId toReplicaId(RpcCommon.ReplicaId replicaId) {
        if (replicaId != null) {
            return ReplicaId.from(replicaId.getGroupName(), replicaId.getNodeId());
        }
        return null;
    }

    public static RpcCommon.ReplicaId protoReplicaId(ReplicaId replicaId) {
        RpcCommon.ReplicaId protoReplicaId = RpcCommon.ReplicaId.newBuilder()
                .setGroupName(replicaId.getGroupName())//
                .setNodeId(replicaId.getNodeId())//
                .build();
        return protoReplicaId;
    }


    public static List<LogEntry> parseLogEntries(long prevLogIndex, final List<RpcCommon.LogEntryMeta> logEntryMetas, final ByteString logEntriesData) {
        if (logEntriesData == null || logEntryMetas == null || logEntryMetas.isEmpty()) {
            return Collections.emptyList();
        }
        final ByteBuffer allData = logEntriesData.asReadOnlyByteBuffer();
        List<LogEntry> logEntries = new ArrayList<>(logEntryMetas.size());
        for (RpcCommon.LogEntryMeta logEntryMeta : logEntryMetas) {
            final long logIndex = ++prevLogIndex;
            final LogEntry logEntry = parseLogEntry(logIndex, logEntryMeta, allData);
            logEntries.add(logEntry);
        }
        return logEntries;
    }

    public static LogEntry parseLogEntry(final long logIndex, final RpcCommon.LogEntryMeta logEntryMeta, final ByteBuffer allData) {
        final LogEntry.Type type = toLogEntryType(logEntryMeta.getType());
        Objects.requireNonNull(type, "log entry type");
        final LogEntry logEntry = new LogEntry(type);
        logEntry.setLogIndex(logIndex);
        logEntry.setLogTerm(logEntryMeta.getLogTerm());
        if (logEntryMeta.hasChecksum()) {
            logEntry.setChecksum(logEntryMeta.getChecksum());
        }
        final int dataLen = logEntryMeta.getDataLen();
        if (dataLen > 0) {
            //TODO split block??
            final byte[] logData = new byte[dataLen];
            allData.get(logData, 0, dataLen);
            logEntry.setLogData(ByteBuffer.wrap(logData));
        }
        return logEntry;
    }

    public static LogEntry.Type toLogEntryType(final RpcCommon.LogEntryType type) {
        switch (type) {
            case OP_DATA:
                return LogEntry.Type.OP_DATA;
            case NO_OP:
                return LogEntry.Type.NO_OP;
            default:
                return null;
        }
    }

    public static RpcCommon.LogEntryType protoLogEntryType(final LogEntry.Type type) {
        switch (type) {
            case OP_DATA:
                return RpcCommon.LogEntryType.OP_DATA;
            case NO_OP:
                return RpcCommon.LogEntryType.NO_OP;
            default:
                return null;
        }
    }

    public static RpcRequest.ErrorResponse toErrorResponse(final PacificaException pacificaException) {
        return RpcRequest.ErrorResponse//
                .newBuilder()//
                .setCode(pacificaException.getCode().getCode())//
                .setMessage(pacificaException.getMessage())//
                .build();
    }

    public static PacificaException toPacificaException(final RpcRequest.ErrorResponse errorResponse) {
        final int code = errorResponse.getCode();
        final String msg = errorResponse.getMessage();
        return new PacificaException(PacificaErrorCode.fromCode(code), msg);
    }

    public static String toLogInfo(RpcRequest.AppendEntriesRequest request) {
        StringBuilder infoBuilder = new StringBuilder("AppendEntriesRequest[");
        infoBuilder.append("primary_id=").append(toReplicaId(request.getPrimaryId())).append(",");
        infoBuilder.append("target_id=").append(toReplicaId(request.getTargetId())).append(",");
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
