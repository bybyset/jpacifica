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
import com.trs.pacifica.LogStorage;
import com.trs.pacifica.LogStorageFactory;
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.log.codec.DefaultLogEntryCodecFactory;
import com.trs.pacifica.log.codec.LogEntryCodecFactory;
import com.trs.pacifica.log.codec.LogEntryDecoder;
import com.trs.pacifica.log.codec.LogEntryEncoder;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.test.BaseStorageTest;
import com.trs.pacifica.test.MockSingleThreadExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class LogManagerImplTest {

    private LogManagerImpl logManager;
    private ReplicaImpl replica;
    private StateMachineCaller stateMachineCaller;

    private LogStorage logStorage;
    private LogStorageFactory logStorageFactory;

    private ReplicaOption replicaOption;

    private LogEntryCodecFactory logEntryCodecFactory = new DefaultLogEntryCodecFactory();
    private SingleThreadExecutor singleThreadExecutor = new MockSingleThreadExecutor();


    @BeforeEach
    public void setup() throws Exception {
        this.replica = Mockito.mock(ReplicaImpl.class);
        Mockito.when(this.replica.getReplicaId()).thenReturn(new ReplicaId("test_group", "test_node"));
        this.stateMachineCaller = Mockito.mock(StateMachineCaller.class);

        mockLogStorage();
        this.logStorageFactory = Mockito.mock(LogStorageFactory.class);
        Mockito.doReturn(this.logStorage).when(this.logStorageFactory).newLogStorage(Mockito.anyString(), Mockito.any(LogEntryEncoder.class), Mockito.any(LogEntryDecoder.class));

        this.replicaOption = Mockito.mock(ReplicaOption.class);
        Mockito.doReturn(true).when(this.replicaOption).isEnableLogEntryChecksum();
        Mockito.doReturn(0).when(this.replicaOption).getSnapshotLogIndexReserved();

        this.logManager = new LogManagerImpl(this.replica);
        LogManagerImpl.Option option = new LogManagerImpl.Option();
        option.setLogStoragePath("test path");
        option.setLogManagerExecutor(singleThreadExecutor);
        option.setStateMachineCaller(this.stateMachineCaller);
        option.setLogStorageFactory(this.logStorageFactory);
        option.setLogEntryCodecFactory(this.logEntryCodecFactory);
        option.setReplicaOption(replicaOption);

        this.logManager.init(option);
        this.logManager.startup();
    }


    @Test
    public void testStartupState() {
        final LogId firstLogId = new LogId(4, 1);
        Assertions.assertEquals(firstLogId, this.logManager.getFirstLogId());
        final LogId lastLogId = new LogId(20, 1);
        Assertions.assertEquals(lastLogId, this.logManager.getLastLogId());
        Assertions.assertNotNull(this.logManager.getLogEntryAt(20));
        Assertions.assertNotNull(this.logManager.getLogEntryAt(4));
        Assertions.assertNull(this.logManager.getLogEntryAt(3));
        Assertions.assertNull(this.logManager.getLogEntryAt(0));
        Assertions.assertNull(this.logManager.getLogEntryAt(21));

        Assertions.assertEquals(0, this.logManager.getLogTermAt(21));
        Assertions.assertEquals(0, this.logManager.getLogTermAt(1));
        Assertions.assertEquals(1, this.logManager.getLogTermAt(4));
        Assertions.assertEquals(1, this.logManager.getLogTermAt(20));



    }


    @Test
    public void testAppendLogEntriesOnPrimary() throws InterruptedException {

        List<LogEntry> logEntries = new ArrayList<>();
        for (long logIndex = 21; logIndex <= 25; logIndex++) {
            LogId logId = new LogId(logIndex, 0);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            logEntries.add(logEntry);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Finished> result = new AtomicReference<>(null);
        LogManager.AppendLogEntriesCallback logEntriesCallback = new LogManager.AppendLogEntriesCallback() {
            @Override
            public void run(Finished finished) {
                result.set(finished);
                countDownLatch.countDown();

            }
        };
        this.logManager.appendLogEntries(logEntries, logEntriesCallback);
        countDownLatch.await();
        Assertions.assertTrue(result.get().isOk());
        Assertions.assertEquals(4, this.logManager.getFirstLogIndex());
        Assertions.assertEquals(25, this.logManager.getLastLogIndex());
        Assertions.assertEquals(21, logEntriesCallback.getFirstLogIndex());
        Assertions.assertEquals(5, logEntriesCallback.getAppendCount());
    }

    @Test
    public void testAppendLogEntriesOnSecondary() throws InterruptedException {

        List<LogEntry> logEntries = new ArrayList<>();
        for (long logIndex = 21; logIndex <= 25; logIndex++) {
            LogId logId = new LogId(logIndex, 1);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            logEntries.add(logEntry);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Finished> result = new AtomicReference<>(null);
        LogManager.AppendLogEntriesCallback logEntriesCallback = new LogManager.AppendLogEntriesCallback() {
            @Override
            public void run(Finished finished) {
                result.set(finished);
                countDownLatch.countDown();

            }
        };
        this.logManager.appendLogEntries(logEntries, logEntriesCallback);
        countDownLatch.await();
        Assertions.assertTrue(result.get().isOk());
        Assertions.assertEquals(4, this.logManager.getFirstLogIndex());
        Assertions.assertEquals(25, this.logManager.getLastLogIndex());
        Assertions.assertEquals(21, logEntriesCallback.getFirstLogIndex());
        Assertions.assertEquals(5, logEntriesCallback.getAppendCount());
    }

    @Test
    public void testAppendLogEntriesOnSecondaryHasGap() throws InterruptedException {

        List<LogEntry> logEntries = new ArrayList<>();
        for (long logIndex = 22; logIndex <= 25; logIndex++) {
            LogId logId = new LogId(logIndex, 1);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            logEntries.add(logEntry);
        }

        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Finished> result = new AtomicReference<>(null);
        LogManager.AppendLogEntriesCallback logEntriesCallback = new LogManager.AppendLogEntriesCallback() {
            @Override
            public void run(Finished finished) {
                result.set(finished);
                countDownLatch.countDown();

            }
        };
        this.logManager.appendLogEntries(logEntries, logEntriesCallback);
        countDownLatch.await();

        Assertions.assertNotNull(result.get());
        Assertions.assertInstanceOf(PacificaException.class, result.get().error());
        Assertions.assertEquals(PacificaErrorCode.CONFLICT_LOG, ((PacificaException)(result.get().error())).getCode());
        Assertions.assertEquals(0, logEntriesCallback.getFirstLogIndex());
        Assertions.assertEquals(0, logEntriesCallback.getAppendCount());
    }

    @Test
    public void testAppendLogEntriesOnSecondaryRepetition() throws InterruptedException {

        List<LogEntry> logEntries = new ArrayList<>();
        for (long logIndex = 10; logIndex <= 20; logIndex++) {
            LogId logId = new LogId(logIndex, 1);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            logEntries.add(logEntry);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Finished> result = new AtomicReference<>(null);
        LogManager.AppendLogEntriesCallback logEntriesCallback = new LogManager.AppendLogEntriesCallback() {
            @Override
            public void run(Finished finished) {
                result.set(finished);
                countDownLatch.countDown();

            }
        };
        this.logManager.appendLogEntries(logEntries, logEntriesCallback);
        countDownLatch.await();
        Assertions.assertNotNull(result.get());
        Assertions.assertInstanceOf(PacificaException.class, result.get().error());
        Assertions.assertEquals(PacificaErrorCode.CONFLICT_LOG, ((PacificaException)(result.get().error())).getCode());
        Assertions.assertEquals(0, logEntriesCallback.getFirstLogIndex());
        Assertions.assertEquals(0, logEntriesCallback.getAppendCount());
    }

    @Test
    public void testAppendLogEntriesOnSecondaryLessThanAppliedLogIndex() throws InterruptedException {

        Mockito.doReturn(25L).when(this.stateMachineCaller).getLastAppliedLogIndex();
        List<LogEntry> logEntries = new ArrayList<>();
        for (long logIndex = 21; logIndex <= 25; logIndex++) {
            LogId logId = new LogId(logIndex, 1);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            logEntries.add(logEntry);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Finished> result = new AtomicReference<>(null);
        LogManager.AppendLogEntriesCallback logEntriesCallback = new LogManager.AppendLogEntriesCallback() {
            @Override
            public void run(Finished finished) {
                result.set(finished);
                countDownLatch.countDown();

            }
        };
        this.logManager.appendLogEntries(logEntries, logEntriesCallback);
        countDownLatch.await();
        Assertions.assertNotNull(result.get());
        Assertions.assertInstanceOf(PacificaException.class, result.get().error());
        Assertions.assertEquals(PacificaErrorCode.CONFLICT_LOG, ((PacificaException)(result.get().error())).getCode());
        Assertions.assertEquals(0, logEntriesCallback.getFirstLogIndex());
        Assertions.assertEquals(0, logEntriesCallback.getAppendCount());
    }


    @Test
    public void testAppendLogEntriesOnSecondaryCrossOver() throws InterruptedException {

        List<LogEntry> logEntries = new ArrayList<>();
        for (long logIndex = 18; logIndex <= 25; logIndex++) {
            LogId logId = new LogId(logIndex, 1);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            logEntries.add(logEntry);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Finished> result = new AtomicReference<>(null);
        LogManager.AppendLogEntriesCallback logEntriesCallback = new LogManager.AppendLogEntriesCallback() {
            @Override
            public void run(Finished finished) {
                result.set(finished);
                countDownLatch.countDown();

            }
        };
        this.logManager.appendLogEntries(logEntries, logEntriesCallback);
        countDownLatch.await();
        Assertions.assertTrue(result.get().isOk());
        Assertions.assertEquals(4, this.logManager.getFirstLogIndex());
        Assertions.assertEquals(25, this.logManager.getLastLogIndex());
        Assertions.assertEquals(21, logEntriesCallback.getFirstLogIndex());
        Assertions.assertEquals(5, logEntriesCallback.getAppendCount());
    }

    @Test
    public void testAppendLogEntriesOnSecondaryCrossOverButTruncateSuffix() throws InterruptedException {

        List<LogEntry> logEntries = new ArrayList<>();
        for (long logIndex = 18; logIndex <= 25; logIndex++) {
            LogId logId = new LogId(logIndex, 2);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            logEntries.add(logEntry);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Finished> result = new AtomicReference<>(null);
        LogManager.AppendLogEntriesCallback logEntriesCallback = new LogManager.AppendLogEntriesCallback() {
            @Override
            public void run(Finished finished) {
                result.set(finished);
                countDownLatch.countDown();

            }
        };
        this.logManager.appendLogEntries(logEntries, logEntriesCallback);
        countDownLatch.await();
        Mockito.verify(this.logStorage).truncateSuffix(17);
        Assertions.assertTrue(result.get().isOk());
        Assertions.assertEquals(4, this.logManager.getFirstLogIndex());
        Assertions.assertEquals(25, this.logManager.getLastLogIndex());
        Assertions.assertEquals(18, logEntriesCallback.getFirstLogIndex());
        Assertions.assertEquals(8, logEntriesCallback.getAppendCount());
    }


    @Test
    public void testOnSnapshotTruncatePrefix() throws InterruptedException {
        long snapshotLogIndex = 17;
        long snapshotLogTerm = 1;
        this.logManager.onSnapshot(snapshotLogIndex, snapshotLogTerm);
        this.logManager.flush();
        Assertions.assertEquals(new LogId(snapshotLogIndex, snapshotLogTerm), this.logManager.getLastSnapshotLogId());
        Assertions.assertEquals(18, this.logManager.getFirstLogIndex());
        Assertions.assertEquals(20, this.logManager.getLastLogIndex());
        Mockito.verify(this.logStorage).truncatePrefix(snapshotLogIndex + 1);
    }

    @Test
    public void testOnSnapshotOutOfRangeLogEntryQueue() throws InterruptedException {
        long snapshotLogIndex = 22;
        long snapshotLogTerm = 1;
        Mockito.doReturn(new LogId(0, 0)).when(this.logStorage).truncatePrefix(snapshotLogIndex + 1);
        this.logManager.onSnapshot(snapshotLogIndex, snapshotLogTerm);
        this.logManager.flush();
        Assertions.assertEquals(new LogId(snapshotLogIndex, snapshotLogTerm), this.logManager.getLastSnapshotLogId());
        Assertions.assertEquals(snapshotLogIndex + 1, this.logManager.getFirstLogIndex());
        Assertions.assertEquals(22, this.logManager.getLastLogIndex());
        Mockito.verify(this.logStorage).truncatePrefix(snapshotLogIndex + 1);
    }

    @Test
    public void testOnSnapshotConflicting() throws InterruptedException {
        long snapshotLogIndex = 17;
        long snapshotLogTerm = 2;
        this.logManager.onSnapshot(snapshotLogIndex, snapshotLogTerm);
        this.logManager.flush();
        Assertions.assertEquals(new LogId(snapshotLogIndex, snapshotLogTerm), this.logManager.getLastSnapshotLogId());
        Assertions.assertEquals(snapshotLogIndex + 1, this.logManager.getFirstLogIndex());
        Assertions.assertEquals(snapshotLogIndex, this.logManager.getLastLogIndex());
        Mockito.verify(this.logStorage).reset(snapshotLogIndex + 1);
    }


    private void mockLogStorage() {
        this.logStorage = Mockito.mock(LogStorage.class);
        LogId firstLogId = new LogId(4, 1);
        Mockito.doReturn(firstLogId).when(this.logStorage).getFirstLogId();
        LogId lastLogId = new LogId(20, 1);
        Mockito.doReturn(lastLogId).when(this.logStorage).getLastLogId();
        for (long logIndex = 4; logIndex <= 20; logIndex++) {
            LogId logId = new LogId(logIndex, 1);
            LogEntry logEntry = new LogEntry(logId, LogEntry.Type.OP_DATA);
            Mockito.doReturn(logEntry).when(this.logStorage).getLogEntry(logIndex);
            Mockito.doReturn(logId).when(this.logStorage).getLogIdAt(logIndex);
            Mockito.doReturn(logId).when(this.logStorage).truncatePrefix(logIndex);
            Mockito.doReturn(logId).when(this.logStorage).truncateSuffix(logIndex);
        }
        Mockito.doReturn(true).when(this.logStorage).reset(Mockito.anyLong());

        Mockito.doAnswer(invocation -> {
            return ((List<?>) invocation.getArguments()[0]).size();
        }).when(this.logStorage).appendLogEntries(Mockito.anyList());
    }





}