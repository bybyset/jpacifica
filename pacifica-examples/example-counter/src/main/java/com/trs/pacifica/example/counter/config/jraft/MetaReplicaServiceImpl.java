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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
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

    private <T> void doApplyOperation(final MetaReplicaOperation operation, MetaReplicaClosure<T> closure) {
        OperationClosure<T> operationClosure = new OperationClosure<>() {
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
