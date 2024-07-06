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

package com.trs.pacifica.sender;

import com.trs.pacifica.ConfigurationClient;
import com.trs.pacifica.SnapshotStorage;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.thread.DefaultSingleThreadExecutor;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.core.BallotBoxImpl;
import com.trs.pacifica.core.LogManagerImpl;
import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.core.StateMachineCallerImpl;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.fs.FileService;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.thread.ThreadUtil;
import com.trs.pacifica.util.timer.HashedWheelTimer;
import com.trs.pacifica.util.timer.Timer;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SenderImplTest {

    static final long test_startLogIndex = 1001;
    static final long test_endLogIndex = 1005;
    static final long test_term = 1L;
    static final long test_version = 1L;
    static final long test_commitPoint = 1002L;

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(4);

    private ReplicaId fromId = new ReplicaId("group1", "node1");
    private ReplicaId toId = new ReplicaId("group2", "node2");
    ;
    private SenderImpl sender;
    private SenderImpl.Option option;

    private ReplicaImpl replica;
    private FileService fileService;
    private ConfigurationClient configurationClient;
    private Timer heartBeatTimer = new HashedWheelTimer();

    private BallotBoxImpl ballotBox;
    private ReplicaGroup replicaGroup;
    private StateMachineCallerImpl stateMachineCaller;
    private PacificaClient pacificaClient;
    private LogManagerImpl logManager;
    private SnapshotStorage snapshotStorage;
    private SingleThreadExecutor singleThreadExecutor = new DefaultSingleThreadExecutor(scheduledExecutorService);


    @BeforeEach
    public void setup() throws PacificaException {
        this.replica = Mockito.mock(ReplicaImpl.class);
        this.fileService = Mockito.mock(FileService.class);
        this.configurationClient = Mockito.mock(ConfigurationClient.class);
        this.ballotBox = Mockito.mock(BallotBoxImpl.class);
        this.replicaGroup = Mockito.mock(ReplicaGroup.class);
        this.pacificaClient = Mockito.mock(PacificaClient.class);
        this.stateMachineCaller = Mockito.mock(StateMachineCallerImpl.class);
        this.logManager = Mockito.mock(LogManagerImpl.class);
        this.snapshotStorage = Mockito.mock(SnapshotStorage.class);
        this.option = new SenderImpl.Option();
        option.setReplica(replica);
        option.setFileService(fileService);
        option.setConfigurationClient(configurationClient);
        option.setHeartBeatTimer(heartBeatTimer);
        option.setMaxSendLogEntryBytes(10 * 1024);
        option.setMaxSendLogEntryNum(10);
        option.setBallotBox(ballotBox);
        option.setReplicaGroup(this.replicaGroup);
        option.setPacificaClient(pacificaClient);
        option.setStateMachineCaller(stateMachineCaller);
        option.setLogManager(logManager);
        option.setSnapshotStorage(snapshotStorage);
        option.setSenderExecutor(singleThreadExecutor);
        option.setSenderScheduler(scheduledExecutorService);
        this.sender = new SenderImpl(fromId, toId, SenderType.Candidate);
        this.sender = Mockito.spy(this.sender);
        this.sender.init(option);

        mockStateMachineCaller();
        mockBallotBox();
        mockConfigurationClient();
        mockReplicaGroup();
        mockAppendEntriesRequest();
        mockLogEntries(test_startLogIndex, test_endLogIndex, 1);
    }

    @AfterEach
    public void shutdown() throws PacificaException {
        this.sender.shutdown();
    }


    private void mockConfigurationClient() {
        Mockito.doReturn(true).when(this.configurationClient).addSecondary(Mockito.anyLong(), Mockito.any());
    }
    private void mockBallotBox() {
        Lock lock = new ReentrantLock();
        Mockito.doReturn(lock).when(this.ballotBox).getCommitLock();
        Mockito.doReturn(test_commitPoint).when(this.ballotBox).getLastCommittedLogIndex();
        Mockito.doReturn(true).when(this.ballotBox).ballotBy(Mockito.any(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(true).when(this.ballotBox).recoverBallot(Mockito.any(), Mockito.anyLong());
    }

    private void mockStateMachineCaller() {
        Mockito.doReturn(test_commitPoint).when(this.stateMachineCaller).getLastAppliedLogIndex();
    }
    private void mockReplicaGroup() {
        Mockito.doReturn(test_term).when(this.replicaGroup).getPrimaryTerm();
        Mockito.doReturn(test_version).when(this.replicaGroup).getVersion();
        Mockito.doReturn("test_group").when(this.replicaGroup).getGroupName();
        Mockito.doReturn(fromId).when(this.replicaGroup).getPrimary();
    }

    private void mockLogEntries(long firstLogIndex, long endLogIndex, long term) {
        Mockito.doReturn(new LogId(firstLogIndex, term)).when(this.logManager).getFirstLogId();
        Mockito.doReturn(new LogId(endLogIndex, term)).when(this.logManager).getLastLogId();
        Mockito.doReturn(endLogIndex).when(this.logManager).getLastLogIndex();
        long logIndex = firstLogIndex;
        for (; logIndex <= endLogIndex; logIndex++) {
            Mockito.doReturn(term).when(this.logManager).getLogTermAt(logIndex);
            LogEntry logEntry = new LogEntry(logIndex, term, LogEntry.Type.OP_DATA);
            ByteBuffer logData = ByteBuffer.allocate(8);
            logData.putLong(logIndex);
            logData.flip();
            logEntry.setLogData(logData);
            Mockito.doReturn(logEntry).when(this.logManager).getLogEntryAt(logIndex);
        }
    }

    private void mockAppendEntriesRequest() {
        Mockito.doAnswer(invocation -> {
            RpcRequest.AppendEntriesRequest request = invocation.getArgument(0, RpcRequest.AppendEntriesRequest.class);
            RpcRequestFinished<RpcRequest.AppendEntriesResponse> callback = invocation.getArgument(1, RpcRequestFinished.class);
            long term = request.getTerm();
            long version = request.getVersion();
            long prevLogIndex = request.getPrevLogIndex();
            RpcRequest.AppendEntriesResponse response;
            if (!request.hasLogData()) {
                response = RpcRequest.AppendEntriesResponse.newBuilder()//
                        .setSuccess(true)//
                        .setTerm(term)//
                        .setVersion(version)//
                        .setLastLogIndex(prevLogIndex)//
                        .build();
            } else {
                response = RpcRequest.AppendEntriesResponse.newBuilder()//
                        .setSuccess(true)//
                        .setTerm(term)//
                        .setVersion(version)//
                        .setLastLogIndex(prevLogIndex + request.getLogMetaCount())//
                        .build();
            }
            callback.setRpcResponse(response);
            ThreadUtil.runCallback(callback, Finished.success());
            return null;
        }).when(this.pacificaClient).appendLogEntries(Mockito.any(), Mockito.any(), Mockito.anyInt());


    }


    @Test
    public void testStartup() throws PacificaException {
        this.sender.startup();
        Assertions.assertTrue(this.sender.isStarted());

    }

    @Test
    public void testDoSendProbeRequest() throws PacificaException {
        this.sender.startup();
        this.sender.sendProbeRequest();
        Mockito.verify(this.pacificaClient).appendLogEntries(Mockito.any(), Mockito.any());

    }


    @Test
    public void testSecondaryHandleAppendLogEntryResponseSuccess() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        RpcRequest.AppendEntriesRequest.Builder requestBuilder = mockAppendEntriesRequestBuilder();
        int logEntryCount = 3;
        for (int i = 0; i < logEntryCount; i++) {
            requestBuilder.addLogMeta(RpcCommon.LogEntryMeta.newBuilder().build());
        }
        long lastLogIndex = test_endLogIndex + logEntryCount;
        RpcRequest.AppendEntriesResponse.Builder responseBuilder = mockAppendEntriesResponseBuilder();
        responseBuilder.setSuccess(true);
        responseBuilder.setLastLogIndex(lastLogIndex);
        Assertions.assertTrue(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.success(), responseBuilder.build()));
        Mockito.verify(this.ballotBox).ballotBy(this.toId, test_endLogIndex + 1, lastLogIndex);
        Mockito.verify(this.sender).notifyOnCaughtUp(lastLogIndex);
    }

    @Test
    public void testSecondaryHandleAppendLogEntryResponseError() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        RpcRequest.AppendEntriesRequest.Builder requestBuilder = mockAppendEntriesRequestBuilder();
        int logEntryCount = 3;
        for (int i = 0; i < logEntryCount; i++) {
            requestBuilder.addLogMeta(RpcCommon.LogEntryMeta.newBuilder().build());
        }
        Assertions.assertFalse(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.failure(new RuntimeException("test error")), null));
        // assert block and wait timeout
        Mockito.verify(this.sender).blockUntilTimeout();
    }

    @Test
    public void testSecondaryHandleAppendLogEntryResponseFailureTermMismatch() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        RpcRequest.AppendEntriesRequest.Builder requestBuilder = mockAppendEntriesRequestBuilder();
        int logEntryCount = 3;
        for (int i = 0; i < logEntryCount; i++) {
            requestBuilder.addLogMeta(RpcCommon.LogEntryMeta.newBuilder().build());
        }
        RpcRequest.AppendEntriesResponse.Builder responseBuilder = mockAppendEntriesResponseBuilder();
        responseBuilder.setSuccess(false);
        responseBuilder.setTerm(2L);
        Assertions.assertFalse(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.success(), responseBuilder.build()));
        Mockito.verify(this.replica).onReceiveHigherTerm(2L);
    }

    @Test
    public void testSecondaryHandleAppendLogEntryResponseFailureLogMismatch1() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        RpcRequest.AppendEntriesRequest.Builder requestBuilder = mockAppendEntriesRequestBuilder();
        requestBuilder.setPrevLogIndex(test_endLogIndex);
        requestBuilder.setPrevLogTerm(1L);
        long lastLogIndex = test_endLogIndex - 2;
        RpcRequest.AppendEntriesResponse.Builder responseBuilder = mockAppendEntriesResponseBuilder();
        responseBuilder.setSuccess(false);
        responseBuilder.setLastLogIndex(lastLogIndex); // less than endLogIndex
        Assertions.assertFalse(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.success(), responseBuilder.build()));
        Assertions.assertEquals(lastLogIndex + 1, this.sender.getNextLogIndex());
        Mockito.verify(this.sender, Mockito.times(2)).sendProbeRequest();
    }

    @Test
    public void testSecondaryHandleAppendLogEntryResponseFailureLogMismatch2() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        RpcRequest.AppendEntriesRequest.Builder requestBuilder = mockAppendEntriesRequestBuilder();
        requestBuilder.setPrevLogIndex(test_endLogIndex);
        requestBuilder.setPrevLogTerm(1L);
        long lastLogIndex = test_endLogIndex + 2;
        RpcRequest.AppendEntriesResponse.Builder responseBuilder = mockAppendEntriesResponseBuilder();
        responseBuilder.setSuccess(false);
        responseBuilder.setLastLogIndex(lastLogIndex); // less than endLogIndex
        Assertions.assertFalse(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.success(), responseBuilder.build()));
        Assertions.assertEquals(test_endLogIndex, this.sender.getNextLogIndex());
        Mockito.verify(this.sender, Mockito.times(2)).sendProbeRequest();
        Assertions.assertFalse(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.success(), responseBuilder.build()));
        Assertions.assertEquals(test_endLogIndex - 1, this.sender.getNextLogIndex());
        Mockito.verify(this.sender, Mockito.times(3)).sendProbeRequest();

    }

    @Test
    public void testSecondaryHandleInstallSnapshotResponseSuccess() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        long snapshotLogIndex = 1000;
        RpcRequest.InstallSnapshotRequest request = RpcRequest.InstallSnapshotRequest.newBuilder()
                .setTerm(1L)//
                .setVersion(1L)//
                .setSnapshotLogIndex(snapshotLogIndex)//
                .setSnapshotLogTerm(1L)//
                .build();
        RpcRequest.InstallSnapshotResponse response = RpcRequest.InstallSnapshotResponse.newBuilder()
                .setSuccess(true)//
                .setTerm(1L)//
                .setVersion(1L)//
                .build();
        Assertions.assertTrue(this.sender.handleInstallSnapshotResponse(request, Finished.success(), response));
        Assertions.assertEquals(snapshotLogIndex + 1, this.sender.getNextLogIndex());
    }


    @Test
    public void testSecondaryHandleInstallSnapshotResponseError() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        RpcRequest.InstallSnapshotRequest request = RpcRequest.InstallSnapshotRequest.newBuilder()
                .build();
        Assertions.assertFalse(this.sender.handleInstallSnapshotResponse(request, Finished.failure(new RuntimeException("test error")), null));
        Mockito.verify(this.sender).blockUntilTimeout();
    }

    @Test
    public void testSecondaryHandleInstallSnapshotResponseFailureTermMismatch() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        long snapshotLogIndex = 1000;
        RpcRequest.InstallSnapshotRequest request = RpcRequest.InstallSnapshotRequest.newBuilder()
                .setTerm(1L)//
                .setVersion(1L)//
                .setSnapshotLogIndex(snapshotLogIndex)//
                .setSnapshotLogTerm(1L)//
                .build();
        RpcRequest.InstallSnapshotResponse response = RpcRequest.InstallSnapshotResponse.newBuilder()
                .setSuccess(false)//
                .setTerm(2L)//
                .setVersion(1L)//
                .build();
        Assertions.assertFalse(this.sender.handleInstallSnapshotResponse(request, Finished.success(), response));
        Mockito.verify(this.replica).onReceiveHigherTerm(2L);
    }

    @Test
    public void testSecondaryHandleInstallSnapshotResponseFailure() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        long snapshotLogIndex = 1000;
        RpcRequest.InstallSnapshotRequest request = RpcRequest.InstallSnapshotRequest.newBuilder()
                .setTerm(1L)//
                .setVersion(1L)//
                .setSnapshotLogIndex(snapshotLogIndex)//
                .setSnapshotLogTerm(1L)//
                .build();
        RpcRequest.InstallSnapshotResponse response = RpcRequest.InstallSnapshotResponse.newBuilder()
                .setSuccess(false)//
                .setTerm(1L)//
                .setVersion(1L)//
                .build();
        Assertions.assertFalse(this.sender.handleInstallSnapshotResponse(request, Finished.success(), response));
        Mockito.verify(this.sender).blockUntilTimeout();
    }


    @Test
    public void testFillCommonRequest() {
        RpcRequest.AppendEntriesRequest.Builder builder = RpcRequest.AppendEntriesRequest.newBuilder();
        long prevLogIndex = -1L;
        Assertions.assertFalse(this.sender.fillCommonRequest(builder, prevLogIndex, false));
        prevLogIndex = test_startLogIndex - 1;
        builder = RpcRequest.AppendEntriesRequest.newBuilder();
        mockLogEntries(test_startLogIndex, test_endLogIndex, 1);
        Mockito.doReturn(0L).when(this.logManager).getLogTermAt(prevLogIndex);
        Assertions.assertFalse(this.sender.fillCommonRequest(builder, prevLogIndex, false));
        builder = RpcRequest.AppendEntriesRequest.newBuilder();
        Assertions.assertTrue(this.sender.fillCommonRequest(builder, prevLogIndex, true));
        Assertions.assertEquals(0L, builder.getPrevLogIndex());
        Assertions.assertEquals(0L, builder.getPrevLogTerm());

        prevLogIndex = test_startLogIndex;
        Assertions.assertTrue(this.sender.fillCommonRequest(builder, prevLogIndex, false));
        Assertions.assertEquals(prevLogIndex, builder.getPrevLogIndex());
        Assertions.assertEquals(1L, builder.getPrevLogTerm());
        Assertions.assertEquals(test_commitPoint, builder.getCommitPoint());
        Assertions.assertEquals(test_term, builder.getTerm());
        Assertions.assertEquals(test_version, builder.getVersion());

        prevLogIndex = test_startLogIndex;
        Assertions.assertTrue(this.sender.fillCommonRequest(builder, prevLogIndex, true));
        Assertions.assertEquals(prevLogIndex, builder.getPrevLogIndex());
        Assertions.assertEquals(1L, builder.getPrevLogTerm());
        Assertions.assertEquals(test_commitPoint, builder.getCommitPoint());
        Assertions.assertEquals(test_term, builder.getTerm());
        Assertions.assertEquals(test_version, builder.getVersion());

    }


    @Test
    public void testWaitCaughtUpTimeout() throws PacificaException, InterruptedException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.startup();
        final AtomicReference<Finished> ref = new AtomicReference<>(null);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Sender.OnCaughtUp onCaughtUp = new Sender.OnCaughtUp() {
            @Override
            public void run(Finished finished) {
                ref.set(finished);
                countDownLatch.countDown();
            }
        };
        int timeoutMs = 1000;
        long start = System.currentTimeMillis();
        Assertions.assertTrue(this.sender.waitCaughtUp(onCaughtUp, timeoutMs));
        Assertions.assertNotNull(this.sender.getOnCaughtUp());
        countDownLatch.await();
        long end = System.currentTimeMillis();
        Assertions.assertFalse(ref.get().isOk());
        Assertions.assertTrue((end - start) >= timeoutMs);
        Assertions.assertFalse(this.sender.waitCaughtUp(onCaughtUp, timeoutMs));
    }

    @Test
    public void testWaitCaughtUpSuccess() throws PacificaException, InterruptedException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.startup();
        final AtomicReference<Finished> ref = new AtomicReference<>(null);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Sender.OnCaughtUp onCaughtUp = new Sender.OnCaughtUp() {
            @Override
            public void run(Finished finished) {
                ref.set(finished);
                countDownLatch.countDown();
            }
        };
        int timeoutMs = 1000;
        long start = System.currentTimeMillis();
        Assertions.assertTrue(this.sender.waitCaughtUp(onCaughtUp, timeoutMs));
        Assertions.assertNotNull(this.sender.getOnCaughtUp());
        countDownLatch.await();
        long end = System.currentTimeMillis();
        Assertions.assertTrue((end - start) >= timeoutMs);
        Assertions.assertFalse(this.sender.waitCaughtUp(onCaughtUp, timeoutMs));
    }


    @Test
    public void testNotifyOnCaughtUp() throws PacificaException, InterruptedException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.startup();
        final AtomicReference<Finished> ref = new AtomicReference<>(null);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Sender.OnCaughtUp onCaughtUp = new Sender.OnCaughtUp() {
            @Override
            public void run(Finished finished) {
                ref.set(finished);
                countDownLatch.countDown();
            }
        };
        int timeoutMs = 1000;
        Assertions.assertTrue(this.sender.waitCaughtUp(onCaughtUp, timeoutMs));
        long lastLogIndex = 1004;
        this.sender.notifyOnCaughtUp(lastLogIndex);
        countDownLatch.await();
        Assertions.assertTrue(ref.get().isOk());
        Assertions.assertEquals(SenderType.Secondary, this.sender.getType());
        Assertions.assertEquals(lastLogIndex, onCaughtUp.getCaughtUpLogIndex());

    }




    RpcRequest.AppendEntriesRequest.Builder mockAppendEntriesRequestBuilder() {
        return RpcRequest.AppendEntriesRequest.newBuilder()//
                .setPrimaryId(RpcUtil.protoReplicaId(this.fromId))//
                .setTargetId(RpcUtil.protoReplicaId(this.toId))//
                .setTerm(test_term)//
                .setVersion(test_version)//
                .setCommitPoint(1003)//
                .setPrevLogTerm(test_term)//
                .setPrevLogIndex(test_endLogIndex);
    }

    RpcRequest.AppendEntriesResponse.Builder mockAppendEntriesResponseBuilder() {
        return RpcRequest.AppendEntriesResponse.newBuilder()//
                .setTerm(test_term)//
                .setVersion(test_version)//
                .setSuccess(true)//
                .setCommitPoint(1003)//
                .setLastLogIndex(test_endLogIndex);
    }

}
