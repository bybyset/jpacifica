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
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.core.fsm.BaseStateMachine;
import com.trs.pacifica.core.fsm.OperationIterator;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.test.MockSingleThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;


public class StateMachineCallerImplTest {

    private final ReplicaId replicaId = new ReplicaId("test_group", "test_node");
    private StateMachineCallerImpl stateMachineCaller;

    ReplicaImpl replicaImpl = Mockito.mock(ReplicaImpl.class);
    BaseStateMachine stateMachine = Mockito.spy(BaseStateMachine.class);
    LogManager logManager = Mockito.mock(LogManager.class);
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
        option.setBootstrapId(new LogId(4, 1));
        callbacks.resetPendingIndex(4);
        this.stateMachineCaller.init(option);
        this.stateMachineCaller.startup();

    }

    @AfterEach
    public void shutdown() throws PacificaException {
        this.stateMachineCaller.shutdown();
    }


    @Test
    public void testCommitAt() throws InterruptedException {
        this.callbacks.add(null);
        LogId commitPoint = new LogId(5L, 1L);
        final LogEntry log = new LogEntry(commitPoint, LogEntry.Type.OP_DATA);
        Mockito.when(this.logManager.getLogEntryAt(commitPoint.getIndex())).thenReturn(log);
        Mockito.when(this.logManager.getLogTermAt(commitPoint.getIndex())).thenReturn(commitPoint.getTerm());
        final ArgumentCaptor<OperationIterator> itArg = ArgumentCaptor.forClass(OperationIterator.class);
        Assertions.assertTrue(this.stateMachineCaller.commitAt(commitPoint.getIndex()));
        this.stateMachineCaller.flush();
        Assertions.assertEquals(commitPoint.getIndex(), this.stateMachineCaller.getLastCommittedLogIndex());
        Assertions.assertEquals(commitPoint.getIndex(), this.stateMachineCaller.getLastAppliedLogIndex());
        Assertions.assertEquals(commitPoint, this.stateMachineCaller.getCommittedPont());
        Mockito.verify(this.stateMachine).onApply(itArg.capture());
        final OperationIterator it = itArg.getValue();
        Assertions.assertFalse(it.hasNext());
        Assertions.assertEquals(it.getLogIndex(), 5);
    }


    @Test
    public void testCommitAtForNoLogEntry() throws InterruptedException {
        this.callbacks.add(null);
        LogId commitPoint = new LogId(5L, 1L);
        Mockito.when(this.logManager.getLogEntryAt(commitPoint.getIndex())).thenReturn(null);
        Mockito.when(this.logManager.getLogTermAt(commitPoint.getIndex())).thenReturn(commitPoint.getTerm());

        Assertions.assertTrue(this.stateMachineCaller.commitAt(commitPoint.getIndex()));
        this.stateMachineCaller.flush();
        Assertions.assertEquals(commitPoint.getIndex(), this.stateMachineCaller.getLastCommittedLogIndex());
        Assertions.assertEquals(commitPoint.getIndex() - 1, this.stateMachineCaller.getLastAppliedLogIndex());
        Assertions.assertNotNull(this.stateMachineCaller.getError());
        Mockito.verify(this.stateMachine).onError(this.stateMachineCaller.getError());
        Mockito.verify(this.replicaImpl).onError(this.stateMachineCaller.getError());
    }

    @Test
    public void testCommitAtForStateMachineError() throws Exception {
        this.callbacks.add(null);
        LogId commitPoint = new LogId(5L, 1L);
        final LogEntry log = new LogEntry(commitPoint, LogEntry.Type.OP_DATA);
        Mockito.when(this.logManager.getLogEntryAt(commitPoint.getIndex())).thenReturn(log);
        Mockito.when(this.logManager.getLogTermAt(commitPoint.getIndex())).thenReturn(commitPoint.getTerm());
        final ArgumentCaptor<OperationIterator> itArg = ArgumentCaptor.forClass(OperationIterator.class);
        Mockito.doThrow(new Exception("USER ERROR")).when(this.stateMachine).doApply(commitPoint, log.getLogData(), null);
        Assertions.assertTrue(this.stateMachineCaller.commitAt(commitPoint.getIndex()));
        this.stateMachineCaller.flush();
        Assertions.assertEquals(commitPoint.getIndex(), this.stateMachineCaller.getLastCommittedLogIndex());
        Assertions.assertEquals(commitPoint.getIndex() - 1, this.stateMachineCaller.getLastAppliedLogIndex());
        Assertions.assertNotNull(this.stateMachineCaller.getError());
        Assertions.assertEquals("USER ERROR", this.stateMachineCaller.getError().getCause().getMessage());
        Mockito.verify(this.stateMachine).onError(this.stateMachineCaller.getError());
        Mockito.verify(this.replicaImpl).onError(this.stateMachineCaller.getError());

        Mockito.verify(this.stateMachine).onApply(itArg.capture());
        final OperationIterator it = itArg.getValue();
        Assertions.assertFalse(it.hasNext());
        Assertions.assertEquals(it.getLogIndex(), 5);

    }

    @Test
    public void testCommitAtMultipleLogEntry() throws InterruptedException {
        mockLogEntries(5L, 3, LogEntry.Type.OP_DATA);
        mockLogEntries(8L, 1, LogEntry.Type.NO_OP);
        mockLogEntries(9L, 4, LogEntry.Type.OP_DATA);
        for (int i = 0; i < 8; i++) {
            this.callbacks.add(null);
        }
        Assertions.assertTrue(this.stateMachineCaller.commitAt(12));
        this.stateMachineCaller.flush();
        Assertions.assertEquals(12, this.stateMachineCaller.getLastCommittedLogIndex());
        Assertions.assertEquals(12, this.stateMachineCaller.getLastAppliedLogIndex());
    }

    private void mockLogEntries(long startLogIndex, int count, LogEntry.Type type) {
        for (int i = 0; i < count; i++) {
            LogId commitPoint = new LogId(startLogIndex + i, 1L);
            final LogEntry log = new LogEntry(commitPoint, type);
            Mockito.when(this.logManager.getLogEntryAt(commitPoint.getIndex())).thenReturn(log);
            Mockito.when(this.logManager.getLogTermAt(commitPoint.getIndex())).thenReturn(commitPoint.getTerm());
        }
    }


    @Test
    public void testOnSnapshotLoad() throws InterruptedException {
        StateMachineCaller.SnapshotLoadCallback callback = Mockito.mock(StateMachineCaller.SnapshotLoadCallback.class);
        LogId snapshotLogId = new LogId(13, 1);
        SnapshotReader snapshotReader = Mockito.mock(SnapshotReader.class);
        Mockito.when(snapshotReader.getSnapshotLogId()).thenReturn(snapshotLogId);
        Mockito.when(callback.getSnapshotReader()).thenReturn(snapshotReader);
        this.stateMachineCaller.onSnapshotLoad(callback);
        this.stateMachineCaller.flush();
        Assertions.assertEquals(snapshotLogId.getIndex(), this.stateMachineCaller.getLastAppliedLogIndex());
        Assertions.assertEquals(snapshotLogId.getIndex(), this.stateMachineCaller.getLastCommittedLogIndex());
    }

    @Test
    public void testOnSnapshotSave() throws InterruptedException, IOException {
        StateMachineCaller.SnapshotSaveCallback callback = Mockito.mock(StateMachineCaller.SnapshotSaveCallback.class);
        SnapshotWriter snapshotWriter = Mockito.mock(SnapshotWriter.class);
        LogId snapshotLogId = new LogId(4, 1);
        Mockito.when(callback.getSaveLogId()).thenReturn(snapshotLogId);
        Mockito.doReturn(snapshotWriter).when(callback).start(Mockito.any());
        this.stateMachineCaller.onSnapshotSave(callback);
        this.stateMachineCaller.flush();
        Assertions.assertEquals(snapshotLogId.getIndex(), this.stateMachineCaller.getLastAppliedLogIndex());
        Assertions.assertEquals(snapshotLogId.getIndex(), this.stateMachineCaller.getLastCommittedLogIndex());
        Mockito.verify(this.stateMachine).onSnapshotSave(snapshotWriter);
    }


    @Test
    public void testOnError() throws InterruptedException {
        PacificaException pacificaException = new PacificaException(PacificaErrorCode.INTERNAL, "test error");
        this.stateMachineCaller.onError(pacificaException);
        this.stateMachineCaller.flush();
        Assertions.assertEquals(pacificaException, this.stateMachineCaller.getError());

        Mockito.verify(this.replicaImpl).onError(pacificaException);
        Mockito.verify(this.stateMachine).onError(pacificaException);
        Mockito.verify(this.replicaImpl, Mockito.times(1)).onError(pacificaException);

    }

}
