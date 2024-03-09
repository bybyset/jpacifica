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
import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.LogManager;
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcResponseCallback;
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

public class SenderImpl implements Sender, LifeCycle<SenderImpl.Option> {


    static final Logger LOGGER = LoggerFactory.getLogger(SenderImpl.class);

    private final ReplicaId fromId;

    private final ReplicaId toId;

    private SenderType type;

    private Option option;

    private RepeatedTimer heartbeatTimer;

    private volatile long lastResponseTime;
    private volatile long nextLogIndex;

    private volatile State state = State.UNINITIALIZED;




    public SenderImpl(ReplicaId fromId, ReplicaId toId, SenderType type) {
        this.fromId = fromId;
        this.toId = toId;
        this.type = type;
    }

    private void updateLastResponseTime() {
        this.lastResponseTime = TimeUtils.monotonicMs();
    }

    @Override
    public boolean isAlive() {
        return false;
    }

    @Override
    public SenderType getType() {
        return this.type;
    }


    @Override
    public synchronized void init(Option option) {
        if (this.state == State.UNINITIALIZED) {
            this.option = Objects.requireNonNull(option, "option");
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
            this.state = State.STARTED;
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

        this.option.getPacificaClient().installSnapshot(request, new RpcResponseCallbackAdapter<RpcRequest.InstallSnapshotResponse>() {
            @Override
            public void run(Finished finished) {

            }
        });



    }


    private void sendProbeRequest() {
        sendEmptyLogEntries(false);
    }

    private void handleHeartbeatTimeout() {
        if (isStarted() && this.state != State.APPEND_LOGENTRIES) {
            sendHeartbeat();
        }
    }

    private void sendHeartbeat() {
        sendEmptyLogEntries(true);
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

            this.option.getPacificaClient().appendLogEntries(request, new RpcResponseCallbackAdapter<RpcRequest.AppendEntriesResponse>() {
                @Override
                public void run(Finished finished) {

                }
            });

        }

    }

    private void sendLogEntries(final long nextSendingLogIndex) {
        final RpcRequest.AppendEntriesRequest.Builder requestBuilder = RpcRequest.AppendEntriesRequest.newBuilder();
        if (!fillCommonRequest(requestBuilder, nextSendingLogIndex - 1, false)) {
            //not found LogEntry, we will install snapshot

            installSnapshot();
            return;
        }
        // fill meta and log data
        final List<ByteString> allData = new ArrayList<>();
        final int maxSendLogEntryNum = this.option.getMaxSendLogEntryNum();
        final int maxSendLogEntryBytes = this.option.getMaxSendLogEntryBytes();
        int logEntryNum = 0;
        int logEntryBytes = 0;
        do {
            final RpcCommon.LogEntryMeta.Builder metaBuilder = RpcCommon.LogEntryMeta.newBuilder();
            if (!prepareLogEntry(nextSendingLogIndex + logEntryNum, metaBuilder, allData)) {
                //wait more log entry

                this.option.logManager.waitNewLog(0 , null);
                break;
            }
            logEntryNum++;
            logEntryBytes += 0;
            requestBuilder.addLogMeta(metaBuilder.build());
        } while (logEntryNum < maxSendLogEntryNum && logEntryBytes < maxSendLogEntryBytes);

        ByteString logData = ByteString.copyFrom(allData);
        requestBuilder.setLogData(logData);

        final RpcRequest.AppendEntriesRequest request = requestBuilder.build();

        this.option.getPacificaClient().appendLogEntries(request, new RpcResponseCallbackAdapter<RpcRequest.AppendEntriesResponse>() {

            @Override
            public void run(Finished finished) {

            }

        });

        return;
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
        requestBuilder.setCommitPoint(this.option.getStateMachineCaller().getCommitPoint());
        return true;
    }


    public static enum State {
        APPEND_LOGENTRIES,
        INSTALL_SNAPSHOT,
        STARTED,
        UNINITIALIZED,
        SHUTTING,
        SHUTDOWN;
    }

    public static class Option {

        private SingleThreadExecutor senderExecutor;

        private LogManager logManager;

        private StateMachineCaller stateMachineCaller;

        private ReplicaGroup replicaGroup;

        private PacificaClient pacificaClient;

        private int maxSendLogEntryNum;

        /**
         * Maximum number of bytes of AppendLogEntriesRequest
         * The actual number of bytes is likely to exceed this value
         */
        private int maxSendLogEntryBytes;


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
    }


}
