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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.trs.pacifica.example.counter.config.jraft.MetaReplicaOperation;
import com.trs.pacifica.example.counter.config.jraft.OperationClosure;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.util.BitUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaFsm extends StateMachineAdapter {

    static final Logger LOGGER = LoggerFactory.getLogger(ReplicaFsm.class);

    static final String DEFAULT_SNAPSHOT_FILENAME = "_meta_replica_fsm";
    private final Map<String, MetaReplicaGroup> replicas = new ConcurrentHashMap<>();


    @Override
    public void onApply(Iterator iterator) {
        while (iterator.hasNext()) {
            MetaReplicaOperation operation = null;

            OperationClosure closure = null;
            if (iterator.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (OperationClosure) iterator.done();
                operation = closure.getOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iterator.getData();
                operation = MetaReplicaOperation.fromBytes(data.array());
            }
            if (operation != null) {
                replayOperation(operation, closure);
            }
            iterator.next();
        }

    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {

        String snapshotDirPath = reader.getPath();
        File file = new File(snapshotDirPath, DEFAULT_SNAPSHOT_FILENAME);
        if (file.exists() && file.isFile()) {
            try {
                loadFromFile(file);
            } catch (IOException e) {
                LOGGER.error("Failed to load snapshot.", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        String snapshotDirPath = writer.getPath();
        String filename = DEFAULT_SNAPSHOT_FILENAME;
        File file = new File(snapshotDirPath, filename);
        try {
            saveToFile(file);
            writer.addFile(filename);
            done.run(Status.OK());
        } catch (Throwable e) {
            done.run(new Status(RaftError.EIO, e.getMessage()));
        }
    }


    public boolean addSecondary(final String groupName, final String nodeId, final long version) {
        final MetaReplicaGroup metaReplicaGroup = this.replicas.get(groupName);
        if (metaReplicaGroup != null) {
            return metaReplicaGroup.addSecondary(nodeId, version);
        }
        return false;
    }

    public boolean removeSecondary(final String groupName, final String nodeId, final long version) {
        final MetaReplicaGroup metaReplicaGroup = this.replicas.get(groupName);
        if (metaReplicaGroup != null) {
            return metaReplicaGroup.removeSecondary(nodeId, version);
        }
        return false;
    }

    public boolean changePrimary(final String groupName, final String nodeId, final long version) {
        final MetaReplicaGroup metaReplicaGroup = this.replicas.get(groupName);
        if (metaReplicaGroup != null) {
            return metaReplicaGroup.changePrimary(nodeId, version);
        }
        return false;
    }


    public MetaReplicaGroup getReplicaGroup(final String groupName) {
        return this.replicas.get(groupName);
    }

    private Object doReplayOperation(MetaReplicaOperation operation) {
        Objects.requireNonNull(operation, "operation");
        final String groupName = operation.getGroupName();
        final String nodeId = operation.getTargetNodeId();
        final long version = operation.getVersion();
        final int type = operation.getType();
        Object result = null;
        switch (type) {
            case MetaReplicaOperation.OP_TYPE_ADD_SECONDARY: {
                result = addSecondary(groupName, nodeId, version);
                break;
            }

            case MetaReplicaOperation.OP_TYPE_REMOVE_SECONDARY: {
                result = removeSecondary(groupName, nodeId, version);
                break;
            }

            case MetaReplicaOperation.OP_TYPE_CHANGE_PRIMARY: {
                result = changePrimary(groupName, nodeId, version);
                break;
            }
        }
        return result;
    }

    private void replayOperation(MetaReplicaOperation operation, OperationClosure closure) {
        try {
            Object result = doReplayOperation(operation);
            if (closure != null) {
                closure.setResult(result);
                closure.run(Status.OK());
            }
        } catch (Throwable e) {
            if (closure != null) {
                closure.run(new Status(RaftError.ESTATEMACHINE, e.getMessage()));
            }
        }

    }

    private void loadFromFile(File file) throws IOException {
        this.replicas.clear();
        try (FileInputStream inputStream = new FileInputStream(file)){
            byte[] replicaGroupCountBytes = new byte[Integer.BYTES];
            inputStream.read(replicaGroupCountBytes);
            int replicaGroupCount = BitUtil.getInt(replicaGroupCountBytes, 0);
            if (replicaGroupCount > 0) {
                for (int i = 0; i< replicaGroupCount; i++) {
                    byte[] metaReplicaGroupBytesLength = new byte[Integer.BYTES];
                    inputStream.read(metaReplicaGroupBytesLength);
                    int metaReplicaGroupLength = BitUtil.getInt(metaReplicaGroupBytesLength, 0);
                    byte[] metaReplicaGroupBytes = new byte[metaReplicaGroupLength];
                    inputStream.read(metaReplicaGroupBytes);
                    MetaReplicaGroup metaReplicaGroup = MetaReplicaGroup.fromBytes(metaReplicaGroupBytes);
                    this.replicas.put(metaReplicaGroup.getGroupName(), metaReplicaGroup);
                }
            }
        }
    }

    private void saveToFile(File file) throws IOException {
        final int replicaGroupCount = this.replicas.size();
        List<byte[]> saveBytes = new ArrayList<>();
        final byte[] replicaGroupCountBytes = new byte[Integer.BYTES];
        BitUtil.putInt(replicaGroupCountBytes, 0, replicaGroupCount);
        saveBytes.add(replicaGroupCountBytes);
        for (MetaReplicaGroup metaReplicaGroup : this.replicas.values()) {
            byte[] metaReplicaGroupBytes = MetaReplicaGroup.toBytes(metaReplicaGroup);
            byte[] metaReplicaGroupBytesLength = new byte[Integer.BYTES];
            BitUtil.putInt(metaReplicaGroupBytesLength, 0, metaReplicaGroupBytes.length);
            saveBytes.add(metaReplicaGroupBytesLength);
            saveBytes.add(metaReplicaGroupBytes);
        }
        try (FileOutputStream outputStream = new FileOutputStream(file);){
            for (byte[] bytes : saveBytes) {
                outputStream.write(bytes);
            }
        };
    }

}
