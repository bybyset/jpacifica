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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.trs.pacifica.BallotBox;
import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.LogManager;
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.ExecutorResponseCallback;
import com.trs.pacifica.rpc.RpcResponseCallbackAdapter;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.TimeUtils;
import com.trs.pacifica.util.timer.RepeatedTimer;
import com.trs.pacifica.util.timer.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

public class SenderImpl implements Sender, LifeCycle<SenderImpl.Option> {

    static final Logger LOGGER = LoggerFactory.getLogger(SenderImpl.class);

    private final ReplicaId fromId;

    private final ReplicaId toId;

    private final AtomicLong requestIdAllocator = new AtomicLong(0);

    private final PriorityQueue<RpcContext> flyingRpcQueue = new PriorityQueue<>();
    private SenderType type = SenderType.Candidate;

    private Option option;

    private RepeatedTimer heartbeatTimer;

    private volatile long lastResponseTime;
    private volatile long nextLogIndex;

    private volatile State state = State.UNINITIALIZED;

    private SingleThreadExecutor executor;

    public SenderImpl(ReplicaId fromId, ReplicaId toId, SenderType type) {
        this.fromId = fromId;
        this.toId = toId;
        this.type = type;
    }

    private void updateLastResponseTime() {
        this.lastResponseTime = TimeUtils.monotonicMs();
    }


    @Override
    public boolean isAlive(int leasePeriodTimeOutMs) {
        return isAlive(this.lastResponseTime, leasePeriodTimeOutMs);
    }

    private static boolean isAlive(long lastRpcResponseTimestamp, long leasePeriodTimeOutMs) {
        return TimeUtils.monotonicMs() - lastRpcResponseTimestamp < leasePeriodTimeOutMs;
    }

    @Override
    public SenderType getType() {
        return this.type;
    }

    @Override
    public boolean continueSendLogEntries(long endLogIndex) {
        // TODO if wait more log
        if (endLogIndex >= this.nextLogIndex) {
            sendLogEntries();
            return true;
        }
        return false;
    }

    @Override
    public void waitCaughtUp(Callback onCaughtUp) {

    }

    @Override
    public synchronized void init(Option option) {
        if (this.state == State.UNINITIALIZED) {
            this.option = Objects.requireNonNull(option, "option");
            this.executor = Objects.requireNonNull(option.getSenderExecutor(), "sender executor");
            this.heartbeatTimer = new RepeatedTimer("Heartbeat-Timer", option.getHeartbeatTimeoutMs(), option.getHeartBeatTimer()) {
                @Override
                protected void onTrigger() {
                    handleHeartbeatTimeout();
                }
            };
            this.state = State.SHUTDOWN;
        }
    }

    @Override
    public synchronized void startup() {
        if (this.state == State.SHUTDOWN) {
            this.nextLogIndex = this.option.getLogManager().getLastLogId().getIndex() + 1;
            this.updateLastResponseTime();
            this.heartbeatTimer.start();
            this.sendProbeRequest();
        }
    }

    @Override
    public synchronized void shutdown() {
        if (isStarted()) {
            this.state = State.SHUTTING;

            this.state = State.SHUTDOWN;
        }
    }

    public boolean isStarted() {
        return this.state.compareTo(State.STARTED) > 0;
    }


    private void installSnapshot() {

        final RpcRequest.InstallSnapshotRequest.Builder requestBuilder = RpcRequest.InstallSnapshotRequest.newBuilder();
        requestBuilder.setPrimaryId(RpcUtil.protoReplicaId(this.fromId));
        requestBuilder.setTargetId(RpcUtil.protoReplicaId(this.toId));
        requestBuilder.setTerm(this.option.replicaGroup.getPrimaryTerm());
        requestBuilder.setVersion(this.option.replicaGroup.getVersion());

        final RpcRequest.InstallSnapshotRequest request = requestBuilder.build();
        final RpcContext rpcContext = new RpcContext(RpcType.INSTALL_SNAPSHOT, request);
        this.flyingRpcQueue.add(rpcContext);
        try {
            this.option.getPacificaClient().installSnapshot(request, new ExecutorResponseCallback<RpcRequest.InstallSnapshotResponse>(executor) {

                @Override
                protected void doRun(Finished finished) {
                    onRpcResponse(rpcContext, finished, getRpcResponse());

                }
            });
        } catch (Throwable e) {
            onRpcResponse(rpcContext, Finished.failure(e), null);
        }
    }


    private void doSendProbeRequest() {
        sendEmptyLogEntries(false);
    }

    private void handleHeartbeatTimeout() {
        if (isStarted() && this.state != State.APPEND_LOGENTRIES) {
            doSendHeartbeat();
        }
    }




    private void sendEmptyLogEntries(final boolean isHeartbeatRequest) {
        final RpcRequest.AppendEntriesRequest.Builder requestBuilder = RpcRequest.AppendEntriesRequest.newBuilder();
        if (!fillCommonRequest(requestBuilder, nextLogIndex - 1, isHeartbeatRequest)) {
            //not found LogEntry, we will install snapshot
            installSnapshot();
            return;
        }
        final RpcRequest.AppendEntriesRequest request = requestBuilder.build();
        if (isHeartbeatRequest) {

            this.option.getPacificaClient().appendLogEntries(request, new RpcResponseCallbackAdapter<RpcRequest.AppendEntriesResponse>() {
                @Override
                public void run(Finished finished) {

                }
            });

        } else {
            //probe request
            final RpcContext rpcContext = new RpcContext(RpcType.APPEND_LOG_ENTRY, request);
            this.flyingRpcQueue.add(rpcContext);
            try {
                this.option.getPacificaClient().appendLogEntries(request, new ExecutorResponseCallback<RpcRequest.AppendEntriesResponse>(executor) {
                    @Override
                    protected void doRun(Finished finished) {
                        onRpcResponse(rpcContext, finished, this.getRpcResponse());
                    }
                });
            } catch (Throwable e) {
                onRpcResponse(rpcContext, Finished.failure(e), null);
            }
        }

    }
    private void doSendHeartbeat() {
        sendEmptyLogEntries(true);
    }


    private void sendProbeRequest() {
        this.executor.execute(() -> {
            doSendProbeRequest();
        });
    }


    private void sendLogEntries() {
        //
        this.executor.execute(() -> {
            doSendLogEntries(nextLogIndex);
        });
    }

    /**
     *
     * @param nextSendingLogIndex
     * @return true if continue to send or else false
     */
    private boolean doSendLogEntries(final long nextSendingLogIndex) {
        final RpcRequest.AppendEntriesRequest.Builder requestBuilder = RpcRequest.AppendEntriesRequest.newBuilder();
        if (!fillCommonRequest(requestBuilder, nextSendingLogIndex - 1, false)) {
            //not found LogEntry, we will install snapshot
            installSnapshot();
            return false;
        }
        // fill meta and log data
        final List<ByteString> allData = new ArrayList<>();
        final int maxSendLogEntryNum = this.option.getMaxSendLogEntryNum();
        final int maxSendLogEntryBytes = this.option.getMaxSendLogEntryBytes();
        boolean continueSend = true;
        int logEntryNum = 0;
        int logEntryBytes = 0;
        do {
            final RpcCommon.LogEntryMeta.Builder metaBuilder = RpcCommon.LogEntryMeta.newBuilder();
            if (!prepareLogEntry(nextSendingLogIndex + logEntryNum, metaBuilder, allData)) {
                //There are no more logs
                continueSend = false;
                break;
            }
            logEntryNum++;
            logEntryBytes += metaBuilder.getDataLen();
            requestBuilder.addLogMeta(metaBuilder.build());
        } while (logEntryNum < maxSendLogEntryNum && logEntryBytes < maxSendLogEntryBytes);
        if (logEntryNum == 0) {
            // wait more log entry
            return false;
        }

        ByteString logData = ByteString.copyFrom(allData);
        requestBuilder.setLogData(logData);

        final RpcRequest.AppendEntriesRequest request = requestBuilder.build();
        final RpcContext context = new RpcContext(RpcType.APPEND_LOG_ENTRY, request);
        this.flyingRpcQueue.add(context);
        try {
            this.option.getPacificaClient().appendLogEntries(request, new ExecutorResponseCallback<RpcRequest.AppendEntriesResponse>(executor) {
                @Override
                protected void doRun(Finished finished) {
                    onRpcResponse(context, finished, this.getRpcResponse());
                }
            });
        } catch (Throwable e) {
            onRpcResponse(context, Finished.failure(e), null);
        }
        return continueSend;
    }

    private boolean prepareLogEntry(final long logIndex, final RpcCommon.LogEntryMeta.Builder metaBuilder, final List<ByteString> allData) {
        final LogEntry logEntry = this.option.logManager.getLogEntryAt(logIndex);
        if (logEntry == null) {
            return false;
        }
        metaBuilder.setLogTerm(logEntry.getLogId().getTerm());
        metaBuilder.setType(RpcUtil.protoLogEntryType(logEntry.getType()));
        if (logEntry.hasChecksum()) {
            metaBuilder.setChecksum(logEntry.getChecksum());
        }
        final ByteBuffer logData = logEntry.getLogData();
        if (logData != null) {
            int dataLen = logData.remaining();
            metaBuilder.setDataLen(dataLen);
            allData.add(ByteString.copyFrom(logData));
        } else {
            metaBuilder.setDataLen(0);
            allData.add(ByteString.empty());
        }
        return true;
    }


    /**
     * @param requestBuilder
     * @param prevLogIndex   must be >= 0
     * @param isHeartbeat    true if heartbeat request
     * @return true if success
     */
    private boolean fillCommonRequest(final RpcRequest.AppendEntriesRequest.Builder requestBuilder, long prevLogIndex, final boolean isHeartbeat) {
        if (prevLogIndex < 0) {
            return false;
        }
        final LogManager logManager = Objects.requireNonNull(this.option.getLogManager(), "logManager");
        final long prevLogTerm = logManager.getLogTermAt(prevLogIndex);
        if (prevLogTerm == 0 && prevLogIndex > 0) {
            // not found
            if (!isHeartbeat) {
                return false;
            } else {
                prevLogIndex = 0;
            }
        }
        requestBuilder.setPrevLogIndex(prevLogIndex);
        requestBuilder.setPrevLogTerm(prevLogTerm);
        requestBuilder.setPrimaryId(RpcUtil.protoReplicaId(this.fromId));
        requestBuilder.setTargetId(RpcUtil.protoReplicaId(this.toId));
        requestBuilder.setTerm(this.option.getReplicaGroup().getPrimaryTerm());
        requestBuilder.setVersion(this.option.getReplicaGroup().getVersion());
        requestBuilder.setCommitPoint(this.option.getStateMachineCaller().getCommitPoint().getIndex());
        return true;
    }

    private void onRpcResponse(final RpcContext rpcContext, final Finished finished, final Message response) {
        rpcContext.finished = finished;
        rpcContext.response = response;
        handleRpcResponse();
    }

    private void handleRpcResponse() {
        do {
            RpcContext rpcContext = this.flyingRpcQueue.peek();
            if (rpcContext == null || !rpcContext.isFinished()) {
                break;
            }
            final Finished finished = rpcContext.finished;
            if (finished.isOk()) {
                updateLastResponseTime();
            }
            try {
                final RpcType rpcType = rpcContext.rpcType;
                switch (rpcType) {
                    case APPEND_LOG_ENTRY : {
                        handleAppendLogEntryResponse((RpcRequest.AppendEntriesRequest) rpcContext.request, finished, (RpcRequest.AppendEntriesResponse) rpcContext.response);
                        break;
                    }
                    case INSTALL_SNAPSHOT: {
                        handleInstallSnapshotResponse(finished, (RpcRequest.InstallSnapshotResponse) rpcContext.response);
                    }
                }
            } finally {
                this.flyingRpcQueue.poll();
            }

        } while (!this.flyingRpcQueue.isEmpty());
    }

    private void handleAppendLogEntryResponse(final RpcRequest.AppendEntriesRequest request, final Finished finished, RpcRequest.AppendEntriesResponse response) {
        if (!finished.isOk()) {
            // TODO
            return;
        }
        if (!response.getSuccess()) {
            // failure
            // 1: receive a larger term
            if (response.getTerm() > request.getTerm()) {
                //TODO  shutdown  replica.check step down
                this.shutdown();

                return;
            }

            // 2: prev_log_index not match
            if (response.getLastLogIndex() < this.nextLogIndex - 1) {
                //The target replica contains fewer logs than the primary replica
                this.nextLogIndex = response.getLastLogIndex() + 1;
            } else {
                //The target replica may be truncated
                //
                if (this.nextLogIndex > 1) {
                    this.nextLogIndex--;
                }
            }
            this.sendProbeRequest();
            return;
        }
        //success
        final int appendCount = request.getLogMetaCount();
        if (appendCount > 0) {
            if (this.type.isSecondary()) {
                final long endLogIndex = this.nextLogIndex + appendCount - 1;
                this.option.getBallotBox().ballotBy(this.toId, this.nextLogIndex, endLogIndex);
            }
        }

        this.sendLogEntries();
    }

    private void handleInstallSnapshotResponse(Finished finished, RpcRequest.InstallSnapshotResponse response) {

    }


    enum RpcType {
        APPEND_LOG_ENTRY,

        INSTALL_SNAPSHOT;
    }



    class RpcContext implements Comparable<RpcContext> {

        private final long requestId;

        private final RpcType rpcType;

        private final Message request;

        private Message response;

        private Finished finished;

        RpcContext(RpcType rpcType, Message request) {
            this(requestIdAllocator.incrementAndGet(), rpcType, request);
        }

        RpcContext(final long requestId, RpcType rpcType, Message request) {
            this.requestId = requestId;
            this.rpcType = rpcType;
            this.request = request;
        }

        @Override
        public int compareTo(RpcContext o) {
            return 0;
        }

        public boolean isFinished() {
            return this.finished != null;
        }
    }

    public static enum State {
        PROBE,
        WAIT_MORE_LOG_ENTRY,
        APPEND_LOGENTRIES,
        INSTALL_SNAPSHOT,
        STARTED,
        UNINITIALIZED,
        SHUTTING,
        SHUTDOWN;
    }


    public static class Option {

        static final int DEFAULT_MAX_SEND_LOG_ENTRY_NUM = 16;

        static final int DEFAULT_MAX_SEND_LOG_ENTRY_BYTE_SIZE = 2 * 1024 * 1024;

        private SingleThreadExecutor senderExecutor;

        private LogManager logManager;

        private StateMachineCaller stateMachineCaller;

        private ReplicaGroup replicaGroup;

        private PacificaClient pacificaClient;

        private BallotBox ballotBox;

        private int maxSendLogEntryNum = DEFAULT_MAX_SEND_LOG_ENTRY_NUM;

        /**
         * Maximum number of bytes of AppendLogEntriesRequest
         * The actual number of bytes is likely to exceed this value
         */
        private int maxSendLogEntryBytes = DEFAULT_MAX_SEND_LOG_ENTRY_BYTE_SIZE;


        private int heartbeatTimeoutMs = 2000;

        private Timer heartBeatTimer;


        public SingleThreadExecutor getSenderExecutor() {
            return senderExecutor;
        }

        public void setSenderExecutor(SingleThreadExecutor senderExecutor) {
            this.senderExecutor = senderExecutor;
        }

        public LogManager getLogManager() {
            return logManager;
        }

        public void setLogManager(LogManager logManager) {
            this.logManager = logManager;
        }

        public StateMachineCaller getStateMachineCaller() {
            return stateMachineCaller;
        }

        public void setStateMachineCaller(StateMachineCaller stateMachineCaller) {
            this.stateMachineCaller = stateMachineCaller;
        }

        public ReplicaGroup getReplicaGroup() {
            return replicaGroup;
        }

        public void setReplicaGroup(ReplicaGroup replicaGroup) {
            this.replicaGroup = replicaGroup;
        }

        public int getMaxSendLogEntryNum() {
            return maxSendLogEntryNum;
        }

        public void setMaxSendLogEntryNum(int maxSendLogEntryNum) {
            this.maxSendLogEntryNum = Math.max(1, maxSendLogEntryNum);
        }

        public int getMaxSendLogEntryBytes() {
            return maxSendLogEntryBytes;
        }

        public void setMaxSendLogEntryBytes(int maxSendLogEntryBytes) {
            this.maxSendLogEntryBytes = maxSendLogEntryBytes;
        }

        public PacificaClient getPacificaClient() {
            return pacificaClient;
        }

        public void setPacificaClient(PacificaClient pacificaClient) {
            this.pacificaClient = pacificaClient;
        }

        public int getHeartbeatTimeoutMs() {
            return heartbeatTimeoutMs;
        }

        public void setHeartbeatTimeoutMs(int heartbeatTimeoutMs) {
            this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        }

        public Timer getHeartBeatTimer() {
            return heartBeatTimer;
        }

        public void setHeartBeatTimer(Timer heartBeatTimer) {
            this.heartBeatTimer = heartBeatTimer;
        }

        public BallotBox getBallotBox() {
            return ballotBox;
        }

        public void setBallotBox(BallotBox ballotBox) {
            this.ballotBox = ballotBox;
        }
    }


}
