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

package com.trs.pacifica.example.counter.service;

import com.trs.pacifica.Replica;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.example.counter.replica.CounterClosure;
import com.trs.pacifica.example.counter.replica.CounterOperation;
import com.trs.pacifica.example.counter.replica.CounterReplica;
import com.trs.pacifica.example.counter.replica.OperationClosure;
import com.trs.pacifica.model.Operation;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.service.ReplicaService;
import com.trs.pacifica.rpc.service.ReplicaServiceManagerHolder;

import java.nio.ByteBuffer;
import java.util.Objects;

public class CounterServiceImpl implements CounterService {

    private final String nodeId;

    public CounterServiceImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void incrementAndGet(final String groupName, long delta, CounterClosure<Long> done) {
        CounterOperation counterOperation = CounterOperation.createIncrement(delta);
        Replica replica = getReplica(new ReplicaId(groupName, nodeId));
        doApplyOperation(replica, counterOperation, done);
    }

    protected Replica getReplica(ReplicaId replicaId) {
        ReplicaService replicaService = ReplicaServiceManagerHolder.getInstance().getReplicaService(replicaId);
        return (Replica) replicaService;
    }


    private void doApplyOperation(Replica replica, CounterOperation counterOperation, CounterClosure counterClosure) {
        OperationClosure operationClosure = new OperationClosure() {
            @Override
            public void run(Finished finished) {
                if (finished.isOk()) {
                    counterClosure.setResult(getResult());
                }
                counterClosure.run(finished);
            }
        };
        doApplyOperation(replica, counterOperation, operationClosure);
    }

    private void doApplyOperation(Replica replica, CounterOperation counterOperation, OperationClosure operationClosure) {
        Objects.requireNonNull(counterOperation, "operation");
        Operation operation = new Operation();
        operation.setLogData(ByteBuffer.wrap(CounterOperation.toBytes(counterOperation)));
        operation.setOnFinish(operationClosure);
        replica.apply(operation);
    }
}
