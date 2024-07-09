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

package com.trs.pacifica.example.counter.config.jraft.fsm;

import com.trs.pacifica.util.BitUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MetaReplica {

    public static final byte STATE_CANDIDATE = 1;
    public static final byte STATE_SECONDARY = 2;
    public static final byte STATE_PRIMARY = 3;

    private final String nodeId;

    private byte state = STATE_CANDIDATE;


    public MetaReplica(String nodeId) {
        this.nodeId = nodeId;
    }

    public MetaReplica(String nodeId, byte state) {
        this.nodeId = nodeId;
        this.state = state;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setSecondary() {
        this.state = STATE_SECONDARY;
    }

    public void setPrimary() {
        this.state = STATE_PRIMARY;
    }

    public void setCandidate() {
        this.state = STATE_CANDIDATE;
    }

    public boolean isSecondary() {
        return this.state == STATE_SECONDARY;
    }

    public boolean isPrimary() {
        return this.state == STATE_PRIMARY;
    }

    public boolean isCandidate() {
        return this.state == STATE_CANDIDATE;
    }


    static MetaReplica fromBytes(byte[] bytes) {
        int nodeIdBytesLength = BitUtil.getInt(bytes, 0);

        final String nodeId = new String(bytes, Integer.BYTES, nodeIdBytesLength, StandardCharsets.UTF_8);
        final byte state = bytes[bytes.length - 1];
        return new MetaReplica(nodeId, state);
    }

    static byte[] toBytes(MetaReplica metaReplica) {
        byte[] nodeIdBytes = metaReplica.nodeId.getBytes(StandardCharsets.UTF_8);
        byte[] nodeIdLengthBytes = new byte[Integer.BYTES];
        BitUtil.putInt(nodeIdLengthBytes, 0 , nodeIdBytes.length);

        byte[] bytes = new byte[nodeIdLengthBytes.length + nodeIdBytes.length + 1];
        System.arraycopy(nodeIdLengthBytes, 0, bytes, 0, nodeIdLengthBytes.length);
        System.arraycopy(nodeIdBytes, 0, bytes, nodeIdLengthBytes.length, nodeIdBytes.length);
        bytes[nodeIdLengthBytes.length + nodeIdBytes.length] = metaReplica.state;
        return bytes;
    }



}
