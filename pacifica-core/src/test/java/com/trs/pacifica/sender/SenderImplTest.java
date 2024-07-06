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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SenderImplTest {

    static final long startLogIndex = 1001;
    static final long endLogIndex = 1005;
    static final long term = 1L;
    static final long version = 1L;

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

        mockReplicaGroup();
        mockAppendEntriesRequest();
        mockLogEntries(startLogIndex, endLogIndex, 1);
    }

    @AfterEach
    public void shutdown() throws PacificaException {
        this.sender.shutdown();
    }


    private void mockReplicaGroup() {
        Mockito.doReturn(term).when(this.replicaGroup).getPrimaryTerm();
        Mockito.doReturn(version).when(this.replicaGroup).getVersion();
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
        RpcRequest.AppendEntriesResponse.Builder responseBuilder = mockAppendEntriesResponseBuilder();
        responseBuilder.setSuccess(true);
        responseBuilder.setLastLogIndex(endLogIndex + logEntryCount);
        Assertions.assertTrue(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.success(), responseBuilder.build()));
        Mockito.verify(this.ballotBox).ballotBy(this.toId, endLogIndex + 1, endLogIndex + logEntryCount);
    }

    @Test
    public void testSecondaryHandleAppendLogEntryResponseFailure() throws PacificaException {
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).startHeartbeatTimer();
        Mockito.doAnswer(invocation -> {return null;}).when(this.sender).sendProbeRequest();
        this.sender.setType(SenderType.Secondary);
        this.sender.startup();
        RpcRequest.AppendEntriesRequest.Builder requestBuilder = mockAppendEntriesRequestBuilder();
        int logEntryCount = 3;
        for (int i = 0; i < logEntryCount; i++) {
            requestBuilder.addLogMeta(RpcCommon.LogEntryMeta.newBuilder().build());
        }
        Assertions.assertTrue(this.sender.handleAppendLogEntryResponse(requestBuilder.build(), Finished.failure(new RuntimeException("test error")), null));

        // assert block and wait timeout
        Mockito.verify(this.sender).blockUntilTimeout();


    }





    RpcRequest.AppendEntriesRequest.Builder mockAppendEntriesRequestBuilder() {
        return RpcRequest.AppendEntriesRequest.newBuilder()//
                .setPrimaryId(RpcUtil.protoReplicaId(this.fromId))//
                .setTargetId(RpcUtil.protoReplicaId(this.toId))//
                .setTerm(term)//
                .setVersion(version)//
                .setCommitPoint(1003)//
                .setPrevLogTerm(term)//
                .setPrevLogIndex(endLogIndex);
    }

    RpcRequest.AppendEntriesResponse.Builder mockAppendEntriesResponseBuilder() {
        return RpcRequest.AppendEntriesResponse.newBuilder()//
                .setTerm(term)//
                .setVersion(version)//
                .setSuccess(true)//
                .setCommitPoint(1003)//
                .setLastLogIndex(endLogIndex);
    }

}
