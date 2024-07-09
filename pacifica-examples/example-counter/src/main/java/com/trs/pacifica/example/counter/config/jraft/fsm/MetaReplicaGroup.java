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
import org.apache.commons.lang.ArrayUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class MetaReplicaGroup {

    private static final AtomicLongFieldUpdater<MetaReplicaGroup> VERSION_UPDATER =
            AtomicLongFieldUpdater.newUpdater(MetaReplicaGroup.class, "version");

    private final String groupName;

    private MetaReplica primary;

    private Map<String, MetaReplica> secondaries = new HashMap<>();

    /**
     * key: nodeId
     * value: MetaReplica
     */
    private Map<String, MetaReplica> allReplica = new HashMap<>();

    private volatile long version = 0L;

    private volatile long term = 0L;


    public MetaReplicaGroup(String groupName) {
        this.groupName = groupName;
    }

    public MetaReplicaGroup(String groupName, long version, long term, Collection<MetaReplica> replicas) {
        this.groupName = groupName;
        this.version = version;
        this.term = term;
        for (MetaReplica metaReplica : replicas) {
            allReplica.put(metaReplica.getNodeId(), metaReplica);
            if (metaReplica.isPrimary()) {
                this.primary = metaReplica;
            } else if (metaReplica.isSecondary()) {
                this.secondaries.put(metaReplica.getNodeId(), metaReplica);
            }
        }
    }


    public String getGroupName() {
        return groupName;
    }

    boolean writeable(final long reqVersion) {
        return VERSION_UPDATER.compareAndSet(this, reqVersion, reqVersion + 1);
    }

    public boolean addSecondary(final String nodeId, final long reqVersion) {
        final MetaReplica metaReplica = this.allReplica.get(nodeId);
        if (metaReplica == null) {
            return false;
        }
        if (writeable(reqVersion)) {
            metaReplica.setSecondary();
            this.secondaries.put(nodeId, metaReplica);
            return true;
        }
        return false;
    }

    public boolean changePrimary(final String nodeId, final long reqVersion) {
        final MetaReplica newPrimary = this.secondaries.get(nodeId);
        if (newPrimary == null) {
            return false;
        }
        if (writeable(reqVersion)) {
            MetaReplica oldPrimary = this.primary;
            if (oldPrimary == newPrimary) {
                // has been primary
                return true;
            }
            // 1、set new primary
            newPrimary.setPrimary();
            this.primary = newPrimary;
            // 2、set old primary to Candidate
            oldPrimary.setCandidate();
            // 3、inc term
            this.term++;
            return true;
        }
        return false;
    }


    public boolean removeSecondary(final String nodeId, final long reqVersion) {
        MetaReplica metaReplica = this.allReplica.get(nodeId);
        if (metaReplica == null) {
            return false;
        }
        if (writeable(reqVersion)) {
            this.secondaries.remove(nodeId);
            metaReplica.setCandidate();
            return true;
        }
        return false;
    }


    static MetaReplicaGroup fromBytes(byte[] bytes) {
        int offset = 0;
        final long term = BitUtil.getLong(bytes, offset);
        offset += Long.BYTES;
        final long version = BitUtil.getLong(bytes, offset);
        offset += Long.BYTES;

        final int groupNameBytesLength = BitUtil.getInt(bytes, offset);
        offset += Integer.BYTES;
        byte[] groupNameBytes = new byte[groupNameBytesLength];
        System.arraycopy(bytes, offset, groupNameBytes, 0, groupNameBytesLength);
        final String groupName = new String(groupNameBytes, StandardCharsets.UTF_8);
        offset += groupNameBytesLength;

        final int replicaCount = BitUtil.getInt(bytes, offset);
        offset += Integer.BYTES;
        List<MetaReplica> replicas = new ArrayList<>(replicaCount);
        if (replicaCount > 0) {
            final int metaReplicaBytesLength = BitUtil.getInt(bytes, offset);
            offset += Integer.BYTES;
            byte[] metaReplicaBytes = new byte[metaReplicaBytesLength];
            System.arraycopy(bytes, offset, metaReplicaBytes, 0, metaReplicaBytesLength);
            final MetaReplica metaReplica = MetaReplica.fromBytes(metaReplicaBytes);
            replicas.add(metaReplica);
        }
        return new MetaReplicaGroup(groupName, version, term, replicas);
    }

    static byte[] toBytes(MetaReplicaGroup metaReplicaGroup) {
        byte[] bytes = null;
        byte[] termBytes = new byte[Long.BYTES];
        BitUtil.putLong(termBytes, 0, metaReplicaGroup.term);
        byte[] versionBytes = new byte[Long.BYTES];
        BitUtil.putLong(versionBytes, 0, metaReplicaGroup.version);
        bytes = ArrayUtils.addAll(termBytes, versionBytes);

        final byte[] groupNameBytes = metaReplicaGroup.groupName.getBytes(StandardCharsets.UTF_8);
        byte[] groupNameBytesLength = new byte[Integer.BYTES];
        BitUtil.putInt(groupNameBytesLength, 0, groupNameBytes.length);
        bytes = ArrayUtils.addAll(bytes, groupNameBytesLength);
        bytes = ArrayUtils.addAll(bytes, groupNameBytes);

        final int replicaCount = metaReplicaGroup.allReplica.size();
        byte[] replicaCountBytes = new byte[Integer.BYTES];
        BitUtil.putInt(replicaCountBytes, 0, replicaCount);
        bytes = ArrayUtils.addAll(bytes, replicaCountBytes);
        for (MetaReplica metaReplica : metaReplicaGroup.allReplica.values()) {
            byte[] metaReplicaBytes = MetaReplica.toBytes(metaReplica);
            byte[] metaReplicaBytesLength = new byte[Integer.BYTES];
            BitUtil.putInt(metaReplicaBytesLength, 0, metaReplicaBytes.length);
            bytes = ArrayUtils.addAll(bytes, metaReplicaBytes);
        }
        return bytes;
    }





}
