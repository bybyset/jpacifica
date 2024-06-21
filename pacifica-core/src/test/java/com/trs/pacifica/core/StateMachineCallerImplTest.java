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

package com.trs.pacifica.core;

import com.trs.pacifica.LogManager;
import com.trs.pacifica.PendingQueue;
import com.trs.pacifica.StateMachine;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.core.PendingQueueImpl;
import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.core.StateMachineCallerImpl;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.test.MockSingleThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class StateMachineCallerImplTest {


    private final ReplicaId replicaId = new ReplicaId("test_group", "test_node");
    private StateMachineCallerImpl stateMachineCaller;


    @Mock
    ReplicaImpl replicaImpl;
    @Mock
    StateMachine stateMachine;
    @Mock
    LogManager logManager;
    SingleThreadExecutor singleThreadExecutor = new MockSingleThreadExecutor();
    PendingQueue<Callback> callbacks = new PendingQueueImpl<>();

    @BeforeEach
    public void setup() throws PacificaException {
        Mockito.when(replicaImpl.getReplicaId()).thenReturn(this.replicaId);

        this.stateMachineCaller = new StateMachineCallerImpl(this.replicaImpl);

        StateMachineCallerImpl.Option option = new StateMachineCallerImpl.Option();
        option.setStateMachine(this.stateMachine);
        option.setLogManager(this.logManager);
        option.setExecutor(this.singleThreadExecutor);
        option.setCallbackPendingQueue(callbacks);
        this.stateMachineCaller.init(option);
        this.stateMachineCaller.startup();

    }

    @AfterEach
    public void shutdown() throws PacificaException {
        this.stateMachineCaller.shutdown();
    }


    @Test
    public void testCommitAt() throws InterruptedException {
        LogId commitPoint = new LogId(5L, 1L);
        final LogEntry log = new LogEntry(commitPoint, LogEntry.Type.OP_DATA);
        Mockito.when(this.logManager.getLogEntryAt(commitPoint.getIndex())).thenReturn(log);
        Mockito.when(this.logManager.getLogTermAt(commitPoint.getIndex())).thenReturn(1L);

        Assertions.assertTrue(this.stateMachineCaller.commitAt(commitPoint.getIndex()));
        this.stateMachineCaller.flush();

        Assertions.assertEquals(this.stateMachineCaller.getLastCommittedLogIndex(), commitPoint.getIndex());
        Assertions.assertEquals(this.stateMachineCaller.getLastAppliedLogIndex(), 11);



    }




}
