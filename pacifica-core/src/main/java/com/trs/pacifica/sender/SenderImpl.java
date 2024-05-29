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
import com.trs.pacifica.*;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.fs.FileService;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.ExecutorRequestFinished;
import com.trs.pacifica.rpc.RpcRequestFinishedAdapter;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.util.RpcLogUtil;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.SystemPropertyUtil;
import com.trs.pacifica.util.TimeUtils;
import com.trs.pacifica.util.thread.ThreadUtil;
import com.trs.pacifica.util.timer.RepeatedTimer;
import com.trs.pacifica.util.timer.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

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

    private AtomicInteger version = new AtomicInteger(0);

    private SingleThreadExecutor executor;

    private final AtomicReference<OnCaughtUp> caughtUpRef = new AtomicReference<>(null);

    private SnapshotReader snapshotReader = null;

    private ScheduledFuture<?> blockTimer = null;


    public SenderImpl(ReplicaId fromId, ReplicaId toId, SenderType type) {
        this.fromId = fromId;
        this.toId = toId;
        this.type = type;
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
    public synchronized void startup() throws PacificaException {
        if (this.state == State.SHUTDOWN) {
            this.version.incrementAndGet();
            this.nextLogIndex = this.option.getLogManager().getLastLogId().getIndex() + 1;
            this.updateLastResponseTime();
            this.state = State.STARTED;
            this.heartbeatTimer.start();
            this.sendProbeRequest();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} is start up", this);
            }
        }
    }

    @Override
    public synchronized void shutdown() throws PacificaException {
        if (isStarted()) {
            this.state = State.SHUTTING;
            releaseSnapshotReader();
            ScheduledFuture<?> blockTimer = this.blockTimer;
            if (blockTimer != null) {
                blockTimer.cancel(true);
            }
            notifyOnCaughtUp(new PacificaException(PacificaErrorCode.STEP_DOWN, "The sender is shutting"));
            this.state = State.SHUTDOWN;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} is shutdown.", this);
            }
        }
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
        if (endLogIndex >= this.nextLogIndex) {
            sendLogEntries();
            return true;
        }
        return false;
    }

    @Override
    public boolean waitCaughtUp(OnCaughtUp onCaughtUp, final long timeoutMs) {
        if (this.caughtUpRef.get() != null) {
            return false;
        }
        final OnCaughtUpTimeoutAble onCaughtUpTimeoutAble = new OnCaughtUpTimeoutAble(onCaughtUp, timeoutMs, TimeUnit.MILLISECONDS);
        if (!this.caughtUpRef.compareAndSet(null, onCaughtUpTimeoutAble)) {
            return false;
        }
        return true;
    }


    private void doNotifyOnCaughtUp(final Finished finished, final long lastLogIndex) {
        final OnCaughtUp caughtUp = this.caughtUpRef.get();
        if (caughtUp == null) {
            return;
        }
        if (!finished.isOk()) {
            this.caughtUpRef.compareAndSet(caughtUp, null);
            ThreadUtil.runCallback(caughtUp, finished);
            return;
        }
        if (doOnCaughtUp(caughtUp, lastLogIndex)) {
            this.caughtUpRef.compareAndSet(caughtUp, null);
        }
    }

    private void notifyOnCaughtUp(final long lastLogIndex) {
        doNotifyOnCaughtUp(Finished.success(), lastLogIndex);
    }

    private void notifyOnCaughtUp(Throwable failure) {
        doNotifyOnCaughtUp(Finished.failure(failure), -1L);
    }

    private boolean doOnCaughtUp(final OnCaughtUp onCaughtUp, final long caughtUpLogIndex) {
        assert onCaughtUp != null;
        final BallotBox ballotBox = this.option.getBallotBox();
        assert ballotBox != null;
        final Lock commitLock = ballotBox.getCommitLock();
        if (!commitLock.tryLock()) {
            //wait for the next notify until it times out
            return false;
        }
        try {
            if (caughtUpLogIndex < this.option.getBallotBox().getLastCommittedLogIndex()) {
                return false;
            }
            final long version = this.option.getReplicaGroup().getVersion();
            // add Secondary
            if (!this.option.getConfigurationClient().addSecondary(version, toId)) {
                throw new PacificaException(PacificaErrorCode.CONF_CLUSTER, "Failed to add Secondary");
            }
            // join ballot
            if (!ballotBox.recoverBallot(toId, caughtUpLogIndex)) {
                throw new PacificaException(PacificaErrorCode.CONF_CLUSTER, "Failed to join ballot.");
            }
            this.type = SenderType.Secondary;
            onCaughtUp.setCaughtUpLogIndex(caughtUpLogIndex);
            ThreadUtil.runCallback(onCaughtUp, Finished.success());
            return true;
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(onCaughtUp, Finished.failure(throwable));
        } finally {
            commitLock.unlock();
        }
        return false;
    }

    public boolean isStarted() {
        return this.state.compareTo(State.STARTED) <= 0;
    }

    private void ensureStarted() {
        if (!isStarted()) {
            throw new AlreadyClosedException("");
        }
    }

    private void changeState(State newState) {
        if (isStarted()) {
            synchronized (this) {
                if (isStarted()) {
                    this.state = newState;
                    return;
                }
            }
        }
        throw new AlreadyClosedException(String.format("The Sender is not started. state=%s", this.state));
    }

    private void installSnapshot() {
        changeState(State.INSTALL_SNAPSHOT);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} -> {} install snapshot", fromId, toId);
        }
        if (!checkConnected()) {
            LOGGER.warn("send install snapshot request, but the replica({}) is not connected.");
            //block and wait timeout
            blockUntilTimeout();
            return;
        }
        try {
            final SnapshotStorage snapshotStorage = this.option.getSnapshotStorage();
            if (snapshotStorage == null) {
                throw new PacificaException(PacificaErrorCode.REPLICATOR, "Not found SnapshotStorage.");
            }
            closeSnapshotReaderIfExist();
            this.snapshotReader = snapshotStorage.openSnapshotReader();
            if (this.snapshotReader == null) {
                throw new PacificaException(PacificaErrorCode.REPLICATOR, "Not found SnapshotReader.");
            }
            final LogId snapshotLogId = this.snapshotReader.getSnapshotLogId();
            if (snapshotLogId == null) {
                throw new PacificaException(PacificaErrorCode.REPLICATOR, "Not found LogId of last do snapshot.");
            }
            final long readerId = this.snapshotReader.generateReadIdForDownload(option.getFileService());
            final RpcRequest.InstallSnapshotRequest.Builder requestBuilder = RpcRequest.InstallSnapshotRequest.newBuilder();
            requestBuilder.setPrimaryId(RpcUtil.protoReplicaId(this.fromId));
            requestBuilder.setTargetId(RpcUtil.protoReplicaId(this.toId));
            requestBuilder.setTerm(this.option.replicaGroup.getPrimaryTerm());
            requestBuilder.setVersion(this.option.replicaGroup.getVersion());
            requestBuilder.setSnapshotLogIndex(snapshotLogId.getIndex());
            requestBuilder.setSnapshotLogTerm(snapshotLogId.getTerm());
            requestBuilder.setReaderId(readerId);

            final RpcRequest.InstallSnapshotRequest request = requestBuilder.build();
            final RpcContext rpcContext = new RpcContext(RpcType.INSTALL_SNAPSHOT, request);
            this.flyingRpcQueue.add(rpcContext);
            try {
                this.option.getPacificaClient().installSnapshot(request, new ExecutorRequestFinished<RpcRequest.InstallSnapshotResponse>(executor) {

                    @Override
                    protected void doRun(Finished finished) {
                        onRpcResponse(rpcContext, finished, getRpcResponse());

                    }
                });
            } catch (Throwable e) {
                onRpcResponse(rpcContext, Finished.failure(e), null);
            }

        } catch (PacificaException e) {
            LOGGER.error("{} failed to send install snapshot request.", this, e);
            //report error
            this.option.getReplica().onError(e);
        }
    }


    private void doSendProbeRequest() {
        changeState(State.PROBE);
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
        final RpcContext rpcContext = new RpcContext(RpcType.APPEND_LOG_ENTRY, request);
        if (isHeartbeatRequest) {
            // heartbeat request
            this.option.getPacificaClient().appendLogEntries(request, new RpcRequestFinishedAdapter<RpcRequest.AppendEntriesResponse>() {
                @Override
                public void run(Finished finished) {
                    handleHeartbeatResponse(rpcContext, finished, getRpcResponse());
                }
            });
        } else {
            //probe request
            this.flyingRpcQueue.add(rpcContext);
            try {
                this.option.getPacificaClient().appendLogEntries(request, new ExecutorRequestFinished<RpcRequest.AppendEntriesResponse>(executor) {
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

    private void closeSnapshotReaderIfExist() {
        SnapshotReader oldSnapshotReader = this.snapshotReader;
        if (oldSnapshotReader != null) {
            try {
                oldSnapshotReader.close();
            } catch (IOException e) {
                LOGGER.error("{} failed to close SnapshotReader. error_msg={}", this, e.getMessage(), e);
            }
        }
    }

    private void doSendHeartbeat() {
        sendEmptyLogEntries(true);
    }


    private void sendProbeRequest() {
        this.executor.execute(() -> {
            try {
                doSendProbeRequest();
            } catch (Throwable e) {
                LOGGER.error("{} failed to execute doSendProbeRequest.", this, e);
            }
        });
    }


    private void sendLogEntries() {
        //
        this.executor.execute(() -> {
            try {
                doSendLogEntries(nextLogIndex);
            } catch (Throwable e) {
                LOGGER.error("{} failed to execute doSendLogEntries.", this, e);
            }

        });
    }

    /**
     * @param nextSendingLogIndex
     * @return true if continue to send or else false
     */
    private boolean doSendLogEntries(final long nextSendingLogIndex) {
        changeState(State.APPEND_LOGENTRIES);
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
            this.option.getPacificaClient().appendLogEntries(request, new ExecutorRequestFinished<RpcRequest.AppendEntriesResponse>(executor) {
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

    private void handleHeartbeatResponse(final RpcContext rpcContext, final Finished finished, final RpcRequest.AppendEntriesResponse heartbeatResponse) {
        if (rpcContext.isExpired()) {
            LOGGER.warn("{} received expired response, ctx={}", this.fromId, rpcContext);
            return;
        }
        rpcContext.finished = finished;
        rpcContext.response = heartbeatResponse;
        if (finished.isOk()) {
            updateLastResponseTime();
        }
        this.executor.execute(() -> {
            try {
                handleAppendLogEntryResponse((RpcRequest.AppendEntriesRequest) rpcContext.request, finished, heartbeatResponse);
            } catch (Throwable e) {
                LOGGER.error("{} failed to execute handleAppendLogEntryResponse.", SenderImpl.this, e);
            }

        });
    }

    private void onRpcResponse(final RpcContext rpcContext, final Finished finished, final Message response) {
        if (rpcContext.isExpired()) {
            LOGGER.warn("{} received expired response, ctx={}", this.fromId, rpcContext);
            return;
        }
        rpcContext.finished = finished;
        rpcContext.response = response;
        handleRpcResponse();
    }

    private void handleRpcResponse() {
        boolean continueSendLogEntry = false;
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
                    case APPEND_LOG_ENTRY: {
                        continueSendLogEntry = handleAppendLogEntryResponse((RpcRequest.AppendEntriesRequest) rpcContext.request, finished, (RpcRequest.AppendEntriesResponse) rpcContext.response);
                        break;
                    }
                    case INSTALL_SNAPSHOT: {
                        continueSendLogEntry = handleInstallSnapshotResponse((RpcRequest.InstallSnapshotRequest) rpcContext.request, finished, (RpcRequest.InstallSnapshotResponse) rpcContext.response);
                    }
                }
            } finally {
                this.flyingRpcQueue.poll();
            }

        } while (!this.flyingRpcQueue.isEmpty());

        if (continueSendLogEntry) {
            this.sendLogEntries();
        }
    }

    private boolean handleAppendLogEntryResponse(final RpcRequest.AppendEntriesRequest request, final Finished finished, @Nullable RpcRequest.AppendEntriesResponse response) {
        if (!finished.isOk()) {
            LOGGER.warn("The Sender={} receive failure for request={}. Wait and try to send again", this, RpcUtil.toLogInfo(request), finished.error());
            this.blockUntilTimeout();
            return false;
        }
        assert response != null;
        if (!response.getSuccess()) {
            // failure
            // 1: receive a larger term
            if (response.getTerm() > request.getTerm()) {
                LOGGER.error("The Sender={} receive failure response={} for request={}.  ", this, RpcUtil.toLogInfo(response), RpcUtil.toLogInfo(request));
                this.shutdownAndCheckTerm(response.getTerm());
                return false;
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("The Sender={} receive failure response={} for request={}. The probe will continue forward ", this, RpcUtil.toLogInfo(response), RpcUtil.toLogInfo(request));
            }
            this.sendProbeRequest();
            return false;
        }
        //success
        final int appendCount = request.getLogMetaCount();
        if (appendCount > 0) {
            if (this.type.isSecondary()) {
                final long endLogIndex = this.nextLogIndex + appendCount - 1;
                this.option.getBallotBox().ballotBy(this.toId, this.nextLogIndex, endLogIndex);
            }
        }
        if (response.hasLastLogIndex()) {
            final long lastLogIndex = response.getLastLogIndex();
            notifyOnCaughtUp(lastLogIndex);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("The Sender={} receive success response={} for request={}.", this, RpcUtil.toLogInfo(response), RpcUtil.toLogInfo(request));
        }
        return true;
    }

    private boolean handleInstallSnapshotResponse(final RpcRequest.InstallSnapshotRequest request, Finished finished, RpcRequest.InstallSnapshotResponse response) {
        try {
            if (!finished.isOk()) {
                LOGGER.warn("InstallSnapshotRequest({}). Receive response but error, we will block until timeout.", RpcLogUtil.toLogString(request), finished.error());
                blockUntilTimeout();
                return false;
            }
            if (!response.getSuccess()) {
                LOGGER.warn("InstallSnapshotRequest({}). Receive response but failure, we will block until timeout.", RpcLogUtil.toLogString(request));
                blockUntilTimeout();
                return false;
            }
            //success
            this.nextLogIndex = request.getSnapshotLogIndex() + 1;
            LOGGER.info("InstallSnapshotRequest({}), Receive success response.", RpcLogUtil.toLogString(request));
            return true;
        } finally {
            releaseSnapshotReader();
        }

    }


    private boolean checkConnected() {
        if (!this.option.getPacificaClient().checkConnection(toId, true)) {
            // block until time out, will continue
            blockUntilTimeout();
            return false;
        }
        return true;
    }

    private void releaseSnapshotReader() {
        if (this.snapshotReader != null) {
            synchronized (this) {
                if (this.snapshotReader != null) {
                    try {
                        this.snapshotReader.close();
                    } catch (IOException e) {
                        LOGGER.error("{} to {} failed to release snapshot reader.", fromId, toId, e);
                    }
                    this.snapshotReader = null;
                }
            }
        }
    }

    /**
     * block until timeout will continue.
     */
    private void blockUntilTimeout() {
        ensureStarted();
        if (this.blockTimer != null) {
            LOGGER.warn("{} repeat block.", this.fromId.getGroupName());
            return;
        }
        final int delayMs = this.option.getHeartbeatTimeoutMs();
        this.blockTimer = this.option.getSenderScheduler().schedule(() -> {
            handleBlockTimeout();
        }, delayMs, TimeUnit.MILLISECONDS);

    }

    private void handleBlockTimeout() {
        try {
            ensureStarted();
            sendProbeRequest();
        } finally {
            this.blockTimer = null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder infoBuilder = new StringBuilder(this.getClass().getSimpleName())//
                .append("[");
        infoBuilder.append("from").append("=").append(this.fromId).append(",");
        infoBuilder.append("to").append("=").append(this.toId).append(",");
        infoBuilder.append("type").append("=").append(this.type).append(",");
        infoBuilder.append("next_log_index").append(this.nextLogIndex).append(",");
        infoBuilder.append("version").append("=").append(this.version.get());
        infoBuilder.append("]");
        return infoBuilder.toString();
    }


    /**
     * receive higher term
     * shutdown and refresh replica to align term.
     *
     * @param higherTerm
     */
    private void shutdownAndCheckTerm(final long higherTerm) {
        try {
            this.shutdown();
        } catch (Throwable e) {
            LOGGER.error("{} failed to shutdown", this);
        }
        this.option.getReplica().onReceiveHigherTerm(higherTerm);
    }

    class OnCaughtUpTimeoutAble extends OnCaughtUp {

        private final OnCaughtUp wrapper;

        private final AtomicBoolean runOnce = new AtomicBoolean(false);

        private ScheduledFuture<?> timeoutFuture;

        OnCaughtUpTimeoutAble(OnCaughtUp wrapper, long timeout, TimeUnit timeUnit) {
            this.wrapper = wrapper;
            this.timeoutFuture = option.getSenderScheduler().schedule(() -> {
                this.run(Finished.failure(new PacificaException(PacificaErrorCode.TIMEOUT, String.format("%s caught up time out.", toId))));
            }, timeout, timeUnit);
        }

        @Override
        public void run(Finished finished) {
            if (runOnce.compareAndSet(false, true)) {
                try {
                    ThreadUtil.runCallback(wrapper, finished);
                } finally {
                    timeoutFuture.cancel(true);
                }
            } else {
                LOGGER.warn("OnCaughtUpTimeoutAble Repeated call run.", finished.error());
            }
        }
    }


    enum RpcType {
        APPEND_LOG_ENTRY,

        INSTALL_SNAPSHOT;
    }


    class RpcContext implements Comparable<RpcContext> {

        private final long requestId;

        private final RpcType rpcType;

        private final Message request;

        private final int version;

        private Message response;

        private Finished finished;

        RpcContext(RpcType rpcType, Message request) {
            this(requestIdAllocator.incrementAndGet(), rpcType, request, SenderImpl.this.version.get());
        }

        RpcContext(final long requestId, RpcType rpcType, Message request, int version) {
            this.requestId = requestId;
            this.rpcType = rpcType;
            this.request = request;
            this.version = version;
        }

        @Override
        public int compareTo(RpcContext o) {
            return 0;
        }

        public boolean isFinished() {
            return this.finished != null;
        }

        public boolean isExpired() {
            return SenderImpl.this.version.get() > version;
        }

        @Override
        public String toString() {
            return "RpcContext{" +
                    "requestId=" + requestId +
                    ", rpcType=" + rpcType +
                    ", version=" + version +
                    ", request=" + request.getClass().getSimpleName() +
                    '}';
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

        public static final int DEFAULT_MAX_SEND_LOG_ENTRY_NUM = SystemPropertyUtil.getInt("pacifica.max.send.log.entry.num", 16);
        public static final int DEFAULT_MAX_SEND_LOG_ENTRY_BYTE_SIZE = SystemPropertyUtil.getInt("pacifica.max.send.log.entry.bytes", 8 * 1024 * 1024);

        private ReplicaImpl replica;

        private SingleThreadExecutor senderExecutor;

        private LogManager logManager;

        private StateMachineCaller stateMachineCaller;

        private ReplicaGroup replicaGroup;

        private PacificaClient pacificaClient;

        private BallotBox ballotBox;

        private ConfigurationClient configurationClient;

        private SnapshotStorage snapshotStorage;

        private int maxSendLogEntryNum = DEFAULT_MAX_SEND_LOG_ENTRY_NUM;

        /**
         * Maximum number of bytes of AppendLogEntriesRequest
         * The actual number of bytes is likely to exceed this value
         */
        private int maxSendLogEntryBytes = DEFAULT_MAX_SEND_LOG_ENTRY_BYTE_SIZE;


        private int heartbeatTimeoutMs = 2000;

        private Timer heartBeatTimer;

        private ScheduledExecutorService senderScheduler;

        private FileService fileService;

        public ReplicaImpl getReplica() {
            return replica;
        }

        public void setReplica(ReplicaImpl replica) {
            this.replica = replica;
        }

        public FileService getFileService() {
            return fileService;
        }

        public void setFileService(FileService fileService) {
            this.fileService = fileService;
        }

        public SnapshotStorage getSnapshotStorage() {
            return snapshotStorage;
        }

        public void setSnapshotStorage(SnapshotStorage snapshotStorage) {
            this.snapshotStorage = snapshotStorage;
        }

        public ScheduledExecutorService getSenderScheduler() {
            return senderScheduler;
        }

        public void setSenderScheduler(ScheduledExecutorService senderScheduler) {
            this.senderScheduler = senderScheduler;
        }

        public ConfigurationClient getConfigurationClient() {
            return configurationClient;
        }

        public void setConfigurationClient(ConfigurationClient configurationClient) {
            this.configurationClient = configurationClient;
        }

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
