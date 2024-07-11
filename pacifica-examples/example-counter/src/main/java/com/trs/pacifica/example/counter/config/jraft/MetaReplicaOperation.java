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

package com.trs.pacifica.example.counter.config.jraft;

import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.util.BitUtil;
import org.apache.commons.lang.ArrayUtils;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class MetaReplicaOperation {

    public static final int OP_TYPE_ADD_SECONDARY = 1;
    public static final int OP_TYPE_REMOVE_SECONDARY = 2;
    public static final int OP_TYPE_CHANGE_PRIMARY = 3;


    private final int type;

    private final String groupName;

    private final String targetNodeId;

    private final long version;


    public MetaReplicaOperation(int type, String groupName, String targetNodeId, long version) {
        this.type = type;
        this.groupName = groupName;
        this.targetNodeId = targetNodeId;
        this.version = version;
    }

    public int getType() {
        return type;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public long getVersion() {
        return version;
    }

    public static MetaReplicaOperation addSecondaryOperation(final ReplicaId replicaId, final long version) {
        return buildOperation(OP_TYPE_ADD_SECONDARY, replicaId, version);
    }

    public static MetaReplicaOperation removeSecondaryOperation(final ReplicaId replicaId, final long version) {
        return buildOperation(OP_TYPE_REMOVE_SECONDARY, replicaId, version);
    }

    public static MetaReplicaOperation changePrimaryOperation(final ReplicaId replicaId, final long version) {
        return buildOperation(OP_TYPE_CHANGE_PRIMARY, replicaId, version);
    }

    public static MetaReplicaOperation buildOperation(int type, final ReplicaId replicaId, long version) {
        return buildOperation(type, replicaId.getGroupName(), replicaId.getNodeId(), version);
    }

    public static MetaReplicaOperation buildOperation(int type, String groupName, String targetNodeId, long version) {
        return new MetaReplicaOperation(type, groupName, targetNodeId, version);
    }


    public static final byte[] toBytes(MetaReplicaOperation operation) {
        Objects.requireNonNull(operation, "operation");
        byte[] typeBytes = new byte[Integer.BYTES];
        BitUtil.putInt(typeBytes, 0, operation.type);
        byte[] versionBytes = new byte[Long.BYTES];
        BitUtil.putLong(versionBytes, 0, operation.version);

        byte[] groupNameBytes = operation.groupName.getBytes(StandardCharsets.UTF_8);
        byte[] groupNameBytesLength = new byte[Integer.BYTES];
        BitUtil.putInt(groupNameBytesLength, 0, groupNameBytes.length);

        byte[] nodeIdBytes = operation.targetNodeId.getBytes(StandardCharsets.UTF_8);
        byte[] nodeIdBytesLength = new byte[Integer.BYTES];
        BitUtil.putInt(nodeIdBytesLength, 0, nodeIdBytes.length);

        byte[] bytes = ArrayUtils.addAll(typeBytes, versionBytes);
        bytes = ArrayUtils.addAll(bytes, groupNameBytesLength);
        bytes = ArrayUtils.addAll(bytes, groupNameBytes);
        bytes = ArrayUtils.addAll(bytes, nodeIdBytesLength);
        bytes = ArrayUtils.addAll(bytes, nodeIdBytes);
        return bytes;
    }

    public static final MetaReplicaOperation fromBytes(byte[] bytes) {
        int offset = 0;
        final int type = BitUtil.getInt(bytes, offset);
        offset += Integer.BYTES;
        final long version = BitUtil.getLong(bytes, offset);
        offset += Long.BYTES;

        final int groupNameBytesLength = BitUtil.getInt(bytes, offset);
        offset += Integer.BYTES;
        final String groupName = new String(bytes, offset, groupNameBytesLength, StandardCharsets.UTF_8);
        offset += groupNameBytesLength;

        final int nodeIdBytesLength = BitUtil.getInt(bytes, offset);
        offset += Integer.BYTES;
        final String nodeId = new String(bytes, offset, nodeIdBytesLength, StandardCharsets.UTF_8);
        offset += nodeIdBytesLength;

        return new MetaReplicaOperation(type, groupName, nodeId, version);
    }


}
