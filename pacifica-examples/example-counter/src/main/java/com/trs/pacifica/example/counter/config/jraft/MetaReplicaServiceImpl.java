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

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;

import java.nio.ByteBuffer;
import java.util.Objects;

public class MetaReplicaServiceImpl implements MetaReplicaService{

    private final MasterServer masterServer;

    public MetaReplicaServiceImpl(MasterServer masterServer) {
        this.masterServer = masterServer;
    }

    @Override
    public void addSecondary(ReplicaId replicaId, long version, final MetaReplicaClosure<Boolean> closure) {
        MetaReplicaOperation operation = MetaReplicaOperation.addSecondaryOperation(replicaId, version);
        doApplyOperation(operation, closure);
    }

    @Override
    public void removeSecondary(ReplicaId replicaId, long version, MetaReplicaClosure<Boolean> closure) {
        MetaReplicaOperation operation = MetaReplicaOperation.removeSecondaryOperation(replicaId, version);
        doApplyOperation(operation, closure);
    }

    @Override
    public void changePrimary(ReplicaId replicaId, long version, MetaReplicaClosure<Boolean> closure) {
        MetaReplicaOperation operation = MetaReplicaOperation.changePrimaryOperation(replicaId, version);
        doApplyOperation(operation, closure);
    }

    @Override
    public void getReplicaGroup(String groupName, MetaReplicaClosure<ReplicaGroup> closure) {

        this.masterServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if(status.isOk()){
                    closure.setResult(readReplicaGroup(groupName));
                    closure.run(Status.OK());
                    return;
                }
                closure.run(status);

            }
        });
    }

    @Override
    public void addReplica(ReplicaId replicaId, MetaReplicaClosure<Boolean> closure) {
        MetaReplicaOperation operation = MetaReplicaOperation.addReplicaOperation(replicaId);
        doApplyOperation(operation, closure);
    }

    private ReplicaGroup readReplicaGroup(String groupName) {
        return this.masterServer.getReplicaFsm().getReplicaGroup(groupName);
    }

    private <T> void doApplyOperation(final MetaReplicaOperation operation, MetaReplicaClosure<T> closure) {
        OperationClosure<T> operationClosure = new OperationClosure<T>() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    closure.setResult(getResult());
                }
                closure.run(status);
            }
        };
        doApplyOperation(operation, operationClosure);
    }

    private <T> void doApplyOperation(final MetaReplicaOperation operation, OperationClosure<T> done) {
        Objects.requireNonNull(operation, "operation");
        done.setOperation(operation);
        Task task = new Task();
        task.setData(ByteBuffer.wrap(MetaReplicaOperation.toBytes(operation)));
        task.setDone(done);
        masterServer.getNode().apply(task);
    }



}
