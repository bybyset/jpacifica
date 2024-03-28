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

import com.trs.pacifica.*;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.FinishedImpl;
import com.trs.pacifica.async.thread.ExecutorGroup;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.error.PacificaCodeException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.fsm.StateMachineCallerImpl;
import com.trs.pacifica.model.*;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.ExecutorResponseCallback;
import com.trs.pacifica.rpc.RpcResponseCallback;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.sender.SenderGroup;
import com.trs.pacifica.sender.SenderType;
import com.trs.pacifica.util.QueueUtil;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.TimeUtils;
import com.trs.pacifica.util.thread.ThreadUtil;
import com.trs.pacifica.util.timer.RepeatedTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicaImpl implements Replica, ReplicaService, LifeCycle<ReplicaOption> {

    static final Logger LOGGER = LoggerFactory.getLogger(ReplicaImpl.class);

    private final ReplicaId replicaId;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.readLock();

    private ReplicaOption option;

    private final Queue<OperationContext> operationContextQueue = QueueUtil.newMpscQueue();

    private final PendingQueue<Callback> callbackPendingQueue = new PendingQueueImpl<>();

    private volatile ReplicaState state = ReplicaState.Uninitialized;

    private ReplicaGroup replicaGroup;

    private ConfigurationClient configurationClient;

    private PacificaClient pacificaClient;


    private StateMachineCallerImpl stateMachineCaller;

    private LogManagerImpl logManager;

    private SnapshotManagerImpl snapshotManager;

    private SenderGroupImpl senderGroup;

    private BallotBoxImpl ballotBox;

    private ExecutorGroup executorGroup;

    private SingleThreadExecutor applyExecutor;

    private RepeatedTimer gracePeriodTimer;

    private RepeatedTimer leasePeriodTimer;

    private RepeatedTimer snapshotTimer;

    private RepeatedTimer recoverTimer;

    /**
     * last timestamp receive heartbeat request from primary
     */
    private volatile long lastPrimaryVisit = TimeUtils.monotonicMs();

    public ReplicaImpl(ReplicaId replicaId) {
        this.replicaId = replicaId;
    }


    private void initLogManager(ReplicaOption option) {
        final ExecutorGroup logExecutorGroup = Objects.requireNonNull(option.getLogManagerExecutorGroup(), "LogManagerExecutorGroup");
        final PacificaServiceFactory pacificaServiceFactory = Objects.requireNonNull(option.getPacificaServiceFactory(), "pacificaServiceFactory");
        final String logStoragePath = Objects.requireNonNull(option.getLogStoragePath(), "logStoragePath");
        final LogManagerImpl.Option logManagerOption = new LogManagerImpl.Option();
        logManagerOption.setReplicaOption(option);
        logManagerOption.setLogStoragePath(logStoragePath);
        logManagerOption.setLogStorageFactory(pacificaServiceFactory);
        logManagerOption.setLogManagerExecutor(logExecutorGroup.chooseExecutor());
        logManagerOption.setStateMachineCaller(this.stateMachineCaller);
        this.logManager.init(logManagerOption);

    }

    private void initSnapshotManager(ReplicaOption option) {
        final SnapshotManagerImpl.Option snapshotManagerOption = new SnapshotManagerImpl.Option();
        snapshotManagerOption.setSnapshotStorageFactory(option.getPacificaServiceFactory());
        this.snapshotManager.init(snapshotManagerOption);
    }

    private void initSenderGroup(ReplicaOption option) {

        final SenderGroupImpl.Option senderGroupOption = new SenderGroupImpl.Option();
        senderGroupOption.setLogManager(Objects.requireNonNull(this.logManager));
        senderGroupOption.setStateMachineCaller(Objects.requireNonNull(stateMachineCaller));
        this.senderGroup.init(senderGroupOption);
    }


    private void initApplyExecutor(ReplicaOption option) {
        this.executorGroup = Objects.requireNonNull(option.getApplyExecutorGroup(), "executorGroup");
        this.applyExecutor = Objects.requireNonNull(this.executorGroup.chooseExecutor());
    }

    private void initBallotBox(ReplicaOption option) {
        final BallotBoxImpl.Option ballotBoxOption = new BallotBoxImpl.Option();
        ballotBoxOption.setFsmCaller(Objects.requireNonNull(this.stateMachineCaller));
        ballotBox.init(ballotBoxOption);
    }

    private void initStateMachineCall(ReplicaOption option) {
        final StateMachine stateMachine = Objects.requireNonNull(option.getStateMachine(), "state machine");
        final StateMachineCallerImpl.Option fsmOption = new StateMachineCallerImpl.Option();
        fsmOption.setStateMachine(stateMachine);
        fsmOption.setLogManager(Objects.requireNonNull(this.logManager));
        final ExecutorGroup fsmExecutorGroup = Objects.requireNonNull(option.getFsmCallerExecutorGroup(), "fsm executor group");
        fsmOption.setExecutor(Objects.requireNonNull(fsmExecutorGroup.chooseExecutor(), "fsm executor"));
        fsmOption.setCallbackPendingQueue(this.callbackPendingQueue);
        this.stateMachineCaller.init(fsmOption);
    }

    private void initRepeatedTimers(ReplicaOption option) {
        this.gracePeriodTimer = new RepeatedTimer("Grace_Period_Timer_" + this.replicaId.getGroupName(), option.getGracePeriodTimeoutMs()) {
            @Override
            protected void onTrigger() {
                handleGracePeriodTimeout();
            }
        };

        this.leasePeriodTimer = new RepeatedTimer("Lease_Period_Timer_" + this.replicaId.getGroupName(), option.getLeasePeriodTimeoutRatio()) {
            @Override
            protected void onTrigger() {
                handleLeasePeriodTimeout();
            }
        };

        this.snapshotTimer = new RepeatedTimer("Snapshot_Timer_" + this.replicaId.getGroupName(), option.getSnapshotTimeoutMs()) {
            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }
        };

        this.recoverTimer = new RepeatedTimer("Replica_Recover_Timer_" + this.replicaId.getGroupName(), option.getRecoverTimeoutMs()) {
            @Override
            protected void onTrigger() {
                handleRecoverTimeout();
            }
        };


    }

    @Override
    public void init(ReplicaOption option) {
        this.writeLock.lock();
        try {
            if (this.state == ReplicaState.Uninitialized) {
                this.option = Objects.requireNonNull(option, "require option");
                this.configurationClient = Objects.requireNonNull(option.getConfigurationClient(), "configurationClient");
                this.pacificaClient = Objects.requireNonNull(option.getPacificaClient(), "pacificaClient");
                this.logManager = new LogManagerImpl(this);
                this.snapshotManager = new SnapshotManagerImpl(this);
                this.stateMachineCaller = new StateMachineCallerImpl(this);
                this.senderGroup = new SenderGroupImpl(this.pacificaClient);
                this.ballotBox = new BallotBoxImpl();

                initApplyExecutor(option);
                initLogManager(option);
                initStateMachineCall(option);
                initSnapshotManager(option);
                initBallotBox(option);
                initSenderGroup(option);
                initRepeatedTimers(option);
                this.state = ReplicaState.Shutdown;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void startup() {
        this.writeLock.lock();
        try {
            if (this.state == ReplicaState.Shutdown) {

                this.logManager.startup();
                this.stateMachineCaller.startup();
                this.snapshotManager.startup();

                onReplicaStateChange();
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            this.state = ReplicaState.Shutdown;
            stopRecoverTimer();
            stopSnapshotTimer();
            stopGracePeriodTimer();
            stopLeasePeriodTimer();

        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public ReplicaId getReplicaId() {
        return this.replicaId;
    }

    @Override
    public ReplicaState getReplicaState(final boolean block) {
        if (block) {
            this.readLock.lock();
        }
        try {
            return this.state;
        } finally {
            if (block) {
                this.readLock.unlock();
            }
        }
    }

    @Override
    public ReplicaState getReplicaState() {
        return Replica.super.getReplicaState();
    }

    @Override
    public boolean isPrimary(boolean block) {
        return false;
    }

    @Override
    public void apply(Operation operation) {
        Objects.requireNonNull(operation, "param: operation is null");
        ensureActive();
        final LogEntry logEntry = new LogEntry(LogEntry.Type.OP_DATA);
        logEntry.setLogData(operation.getLogData());
        apply(logEntry, operation.getOnFinish());
    }


    @Override
    public void snapshot(Callback onFinish) {
        doSnapshot(onFinish);
    }

    @Override
    public void recover(Callback onFinish) {
        doRecover(onFinish);
    }

    @Override
    public LogId getCommitPoint() {
        return null;
    }

    @Override
    public LogId getSnapshotLogId() {
        return null;
    }

    @Override
    public LogId getFirstLogId() {
        return null;
    }

    @Override
    public LogId getLastLogId() {
        return null;
    }

    @Override
    public RpcRequest.AppendEntriesResponse handleAppendLogEntryRequest(RpcRequest.AppendEntriesRequest request, RpcResponseCallback<RpcRequest.AppendEntriesResponse> callback) throws PacificaException {
        // Secondary or Candidate received AppendEntriesRequest from Primary
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                ThreadUtil.runCallback(callback, Finished.failure(new PacificaCodeException(PacificaErrorCode.UNAVAILABLE, "the replica not active. state="+ this.state)));
                return null;
            }

            final ReplicaId targetId = RpcUtil.toReplicaId(request.getTargetId());
            if (!this.replicaId.equals(targetId)) {
                ThreadUtil.runCallback(callback, Finished.failure(new PacificaCodeException(PacificaErrorCode.UNAVAILABLE, String.format("mismatched target id.expect=%s, actual=%s.", this.replicaId, targetId))));
                return null;
            }

            if (this.replicaGroup.getVersion() < request.getVersion()) {
                // TODO refresh replica group


            }
            final ReplicaId fromReplicaId = RpcUtil.toReplicaId(request.getPrimaryId());
            final ReplicaId primaryReplicaId = this.replicaGroup.getPrimary();
            if (!primaryReplicaId.equals(fromReplicaId)) {
                ThreadUtil.runCallback(callback, Finished.failure(new PacificaCodeException(PacificaErrorCode.UNAVAILABLE, String.format("mismatched primary id. expect=%s, actual=%s.", primaryReplicaId, fromReplicaId))));
                return null;
            }

            final long primaryTerm = this.replicaGroup.getPrimaryTerm();
            if (primaryTerm > request.getTerm()) {
                return RpcRequest.AppendEntriesResponse.newBuilder()//
                        .setSuccess(false)//
                        .setTerm(primaryTerm)//
                        .build();
            }

            updateLastPrimaryVisit();
            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            final long localPrevLogTerm = this.logManager.getLogTermAt(prevLogIndex);
            if (prevLogTerm != localPrevLogTerm) {
                final long lastLogIndex = this.logManager.getLastLogId().getIndex();
                LOGGER.warn("{} reject unmatched term at prevLogIndex={}, request_prev_log_term={}, local_prev_log_term={}", this.replicaId, prevLogIndex, prevLogTerm, localPrevLogTerm);
                return RpcRequest.AppendEntriesResponse.newBuilder()//
                        .setSuccess(false)//
                        .setTerm(primaryTerm)//
                        .setLastLogIndex(lastLogIndex)//
                        .build();
            }

            if (request.hasCommitPoint()) {
                //It is possible that the commit point of the Primary is greater than
                // last log index of the Candidate
                final long commitPoint = Math.min(request.getCommitPoint(), prevLogIndex);
                this.stateMachineCaller.commitAt(commitPoint);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("receive append logEntry request, and commit at {} = min(primaryCommitPoint={}, prevLogIndex={})", commitPoint, request.getCommitPoint(), prevLogIndex);
                }
            }
            if (request.getLogMetaCount() == 0) {
                final long lastLogIndex = this.logManager.getLastLogId().getIndex();
                return RpcRequest.AppendEntriesResponse.newBuilder()//
                        .setSuccess(true)//
                        .setTerm(primaryTerm)//
                        .setLastLogIndex(lastLogIndex)//
                        .build();
            }
            final List<LogEntry> logEntries = RpcUtil.parseLogEntries(prevLogIndex, request.getLogMetaList(), request.getLogData());
            final SecondaryAppendLogEntriesCallback secondaryAppendLogEntriesCallback = new SecondaryAppendLogEntriesCallback(callback);
            this.logManager.appendLogEntries(logEntries, secondaryAppendLogEntriesCallback);
        } finally {
            this.writeLock.unlock();
        }
        return null;
    }

    @Override
    public RpcRequest.ReplicaRecoverResponse handleReplicaRecoverRequest(RpcRequest.ReplicaRecoverRequest request, RpcResponseCallback<RpcRequest.ReplicaRecoverResponse> callback) throws PacificaException {
        return null;
    }

    @Override
    public RpcRequest.InstallSnapshotResponse handleInstallSnapshotRequest(RpcRequest.InstallSnapshotRequest request, RpcResponseCallback<RpcRequest.InstallSnapshotResponse> callback) throws PacificaException {
        return null;
    }

    @Override
    public RpcRequest.GetFileResponse handleGetFileRequest(RpcRequest.GetFileRequest request, RpcResponseCallback<RpcRequest.GetFileResponse> callback) throws PacificaException {
        return null;
    }

    /**
     * apply batch operation only by primary
     * @param oneBatch
     */
    private void applyOperationBatch(List<OperationContext> oneBatch) {
        assert oneBatch != null;
        this.writeLock.lock();
        try {
            //check primary state
            if (this.state != ReplicaState.Primary) {
                final Finished result = FinishedImpl.failure(new IllegalStateException("Is not Primary."));
                oneBatch.forEach(context -> {
                    ThreadUtil.runCallback(context.getCallback(), result);
                });
                return;
            }
            final long curPrimaryTerm = this.replicaGroup.getPrimaryTerm();
            final int count = oneBatch.size();
            final List<LogEntry> logEntries = new ArrayList<>(count);
            final List<Callback> callbackList = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final OperationContext context = oneBatch.get(i);
                final Callback callback = context.callback;
                final LogEntry logEntry = context.logEntry;
                logEntry.getLogId().setTerm(curPrimaryTerm);
                logEntries.add(logEntry);
                callbackList.add(callback);
            }
            //log manager append log
            this.logManager.appendLogEntries(logEntries, new PrimaryAppendLogEntriesCallback(callbackList));

        } finally {
            this.writeLock.unlock();
        }

    }

    private void onReplicaStateChange() {
        ReplicaState oldState, newState;
        this.readLock.lock();
        try {
            oldState = newState = this.state;
            newState = toReplicaState(this.replicaGroup, replicaId);
        } finally {
            this.readLock.unlock();
        }
        if (oldState != newState) {
            LOGGER.info("The state of the replica={} changes from {} to {}.", this.replicaId.getGroupName(), oldState,
                    newState);
            switch (newState) {
                case Primary:
                    becomePrimary();
                    break;
                case Secondary:
                    becomeSecondary();
                    break;
                case Candidate:
                    becomeCandidate();
                    break;
                default:
                    break;
            }
        }

    }

    /**
     *
     */
    private void becomePrimary() {
        this.writeLock.lock();
        try {
            this.state = ReplicaState.Primary;
            stopRecoverTimer();
            stopGracePeriodTimer();
            startLeasePeriodTimer();
            startSnapshotTimer();
            startSenderGroup();
            startBallotBox();

            reconciliation();

        } finally {
            this.writeLock.unlock();
        }
    }

    private void becomeSecondary() {
        this.writeLock.lock();
        try {
            this.state = ReplicaState.Secondary;
            this.ballotBox.shutdown();
            this.senderGroup.shutdown();
            this.stopRecoverTimer();
            this.stopLeasePeriodTimer();
            this.startGracePeriodTimer();
            this.startSnapshotTimer();
            LOGGER.info("The replica({}) has become Secondary.", this.replicaId);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void becomeCandidate() {
        this.writeLock.lock();
        try {
            this.state = ReplicaState.Candidate;
            this.ballotBox.shutdown();
            this.senderGroup.shutdown();
            stopLeasePeriodTimer();
            stopGracePeriodTimer();
            stopSnapshotTimer();
            startRecoverTimer();
            LOGGER.info("The replica({}) has become Candidate.", this.replicaId);
        } finally {
            this.writeLock.unlock();
        }

    }

    private void apply(final LogEntry logEntry, final Callback onFinish) {
        final OperationContext context = new OperationContext(logEntry, onFinish);
        if (this.operationContextQueue.offer(context)) {
            this.applyExecutor.execute(new OperationConsumer());
        }
    }

    /**
     * call by Primary for reconciliation
     * should be in write lock
     */
    private void reconciliation() {
        final LogEntry logEntry = new LogEntry(LogEntry.Type.NO_OP);
        apply(logEntry, new Callback() {
            @Override
            public void run(Finished finished) {
                if (finished.isOk()) {
                    LOGGER.info("Primary({}) success to reconciliation", replicaId);
                }
            }
        });
    }

    private void startBallotBox() {
        this.ballotBox.startup();
    }

    private void startSenderGroup() {
        List<ReplicaId> secondaries = this.replicaGroup.listSecondary();
        for (ReplicaId secondary : secondaries) {
            this.senderGroup.addSenderTo(secondary);
        }
        this.senderGroup.startup();
    }

    private void startGracePeriodTimer() {
        this.gracePeriodTimer.start();
    }

    private void stopGracePeriodTimer() {
        this.gracePeriodTimer.stop();
    }

    private void startLeasePeriodTimer() {
        this.leasePeriodTimer.start();
    }

    private void stopLeasePeriodTimer() {
        this.leasePeriodTimer.stop();
    }

    private void startSnapshotTimer() {
        this.snapshotTimer.start();
    }

    private void stopSnapshotTimer() {
        this.snapshotTimer.stop();
    }

    private void startRecoverTimer() {
        this.recoverTimer.start();
    }

    private void stopRecoverTimer() {
        this.recoverTimer.stop();
    }

    private static ReplicaState toReplicaState(final ReplicaGroup replicaGroup, final ReplicaId replicaId) {
        ReplicaId primary = replicaGroup.getPrimary();
        if (replicaId.equals(primary)) {
            return ReplicaState.Primary;
        }
        List<ReplicaId> secondaries = replicaGroup.listSecondary();
        if (secondaries == null || secondaries.isEmpty()) {
            return ReplicaState.Candidate;
        }
        for (ReplicaId secondary : secondaries) {
            if (secondary.equals(replicaId)) {
                return ReplicaState.Secondary;
            }
        }
        return ReplicaState.Candidate;
    }

    public void updateLastPrimaryVisit() {
        this.setLastPrimaryVisit(TimeUtils.monotonicMs());
    }

    public void setLastPrimaryVisit(long lastPrimaryVisit) {
        this.lastPrimaryVisit = lastPrimaryVisit;
    }

    /**
     *
     * @param monotonicNowMs
     * @return
     */
    private boolean isWithinGracePeriod(long monotonicNowMs) {
        return monotonicNowMs - this.lastPrimaryVisit < this.option.getGracePeriodTimeoutMs();
    }

    /**
     * Whether the current Primary is valid
     * @return true if is valid
     */
    private boolean isCurrentPrimaryValid() {
        return isWithinGracePeriod(TimeUtils.monotonicMs());
    }


    /**
     * The Secondary checks whether the Primary is faulty.
     *  If the Primary is faulty, change Primary.
     */
    private void handleGracePeriodTimeout() {
        this.readLock.lock();
        try {
            if (this.state != ReplicaState.Secondary) {
                return;
            }
            if (isCurrentPrimaryValid()) {
                return;
            }
            LOGGER.info("The faulty Primary({}) was found. we will elect Primary", this.replicaGroup.getPrimary());
        } finally {
            this.readLock.unlock();
        }
        //TODO change primary



    }

    /**
     * The Primary checks whether the Secondary is faulty.
     * If the Secondary is faulty, remove Secondary.
     */
    private void handleLeasePeriodTimeout() {
        this.readLock.lock();
        try {
            if (this.state != ReplicaState.Primary) {
                return;
            }
            final List<ReplicaId> secondaries = this.replicaGroup.listSecondary();
            if (secondaries == null || secondaries.isEmpty()) {
                return;
            }
            final List<ReplicaId> removed = new ArrayList<>(secondaries.size());
            long currentVersion = replicaGroup.getVersion();
            boolean hasRemoveFailure = false;
            for (ReplicaId secondary : secondaries) {
                if (isAlive(secondary)) {
                    return;
                }
                LOGGER.info("The faulty Secondary({}) was found, we will remove it from replica group.", secondary);
                if (this.configurationClient.removeSecondary(currentVersion, secondary)) {
                    //success call config cluster to remove faulty secondary.
                    currentVersion++;
                    removed.add(secondary);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Response was received for successfully removing the Secondary({}) from the config cluster", secondary);
                    }
                } else {
                    //failure
                    hasRemoveFailure = true;
                    LOGGER.warn("Response was received for failure removing the Secondary({}) from the config cluster, current_version={}", secondary, currentVersion);
                    break;
                }
            }
            if (hasRemoveFailure) {
                // TODO step down
                return;
            }
            if (!removed.isEmpty()) {
                //TODO 1、remove sender 2、abandon ballot

            }

        } finally {
            this.readLock.unlock();
        }
    }

    /**
     *
     */
    private void handleSnapshotTimeout() {
        this.readLock.lock();
        try {
            if (!this.state.isActive()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn("The replica not active on handle snapshot timeout, state={}", this.state);
                }
                return;
            }
            // TODO do snapshot


        } finally {
            this.readLock.unlock();
        }

    }

    private void handleRecoverTimeout() {
        this.readLock.lock();
        try {
            if (!this.state.isActive()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn("The replica not active on handle recover timeout, state={}", this.state);
                }
                return;
            }
            //TODO doRecover

        } finally {
            this.readLock.unlock();
        }

    }

    boolean isAlive(final ReplicaId secondary) {
        Objects.requireNonNull(secondary, "secondary");
        return this.senderGroup.isAlive(secondary);
    }

    void ensureActive() {
        final ReplicaState replicaState = this.state;
        if (!replicaState.isActive()) {
            throw new PacificaException(String.format("Current replica state=%s is not active", replicaState));
        }
    }

    private void doSnapshot(final Callback onFinish) {
        this.readLock.lock();
        try {
            ensureActive();
            if (this.snapshotManager != null) {
                this.snapshotManager.doSnapshot(onFinish);
            } else {
                throw new PacificaException("Snapshot is not supported");
            }
        } catch (Throwable e) {
            ThreadUtil.runCallback(onFinish, Finished.failure(e));
        } finally {
            this.readLock.unlock();
        }
    }

    private void doRecover(final Callback onFinish) {
        this.readLock.lock();
        try {
            ensureActive();
            //TODO  check recovering
            // refresh

            if (this.state != ReplicaState.Candidate) {
                throw new PacificaException("Only Candidate state needs to recover. current state is " + this.state);
            }
            final ReplicaId primaryId = this.replicaGroup.getPrimary();
            final long term = this.replicaGroup.getPrimaryTerm();
            RpcRequest.ReplicaRecoverRequest recoverRequest = RpcRequest.ReplicaRecoverRequest.newBuilder()//
                    .setPrimaryId(RpcUtil.protoReplicaId(primaryId))//
                    .setRecoverId(RpcUtil.protoReplicaId(this.replicaId))//
                    .setTerm(term)//
                    .build();
            this.pacificaClient.recoverReplica(recoverRequest, new ExecutorResponseCallback<RpcRequest.ReplicaRecoverResponse>(){

                @Override
                protected void doRun(Finished finished) {
                    handleRecoverReplicaResponse(finished, getRpcResponse(), onFinish);
                }
            });

        } catch (Throwable e) {
            ThreadUtil.runCallback(onFinish, Finished.failure(e));
        } finally {
            this.readLock.unlock();
        }
    }

    private void handleRecoverReplicaResponse(final Finished finished, final RpcRequest.ReplicaRecoverResponse response, final @Nullable Callback onFinish) {
        if (!finished.isOk()) {
            ThreadUtil.runCallback(onFinish, finished);
            return;
        }
        assert response != null;
        if (!response.getSuccess()) {

            return;
        }

        //TODO success

    }

    /**
     * TODO recycle
     */
    static class OperationContext {

        final LogEntry logEntry;

        final Callback callback;

        int expectedTerm = -1;

        public OperationContext(LogEntry logEntry, Callback callback) {
            this.logEntry = logEntry;
            this.callback = callback;
        }

        public LogEntry getLogEntry() {
            return this.logEntry;
        }


        public Callback getCallback() {
            return callback;
        }

    }

    class PrimaryAppendLogEntriesCallback extends LogManager.AppendLogEntriesCallback {

        private final List<Callback> failureCallbacks;

        PrimaryAppendLogEntriesCallback(List<Callback> failureCallbacks) {
            this.failureCallbacks = failureCallbacks;
        }


        @Override
        public void run(final Finished finished) {
            long logIndex = this.getFirstLogIndex();
            final long lastLogIndex = logIndex + this.getAppendCount() - 1;
            int appendCount = this.getAppendCount();
            int index = 0;
            for (; index < this.getAppendCount(); index++) {
                final Callback callback = this.failureCallbacks.get(index);
                //initiate ballot to ballotBox
                if (!ReplicaImpl.this.ballotBox.initiateBallot(ReplicaImpl.this.replicaGroup)) {
                    ThreadUtil.runCallback(callback, Finished.failure(new PacificaException(String.format("replica=%s failed to initiate ballot", ReplicaImpl.this.replicaId))));
                    continue;
                }
                if (!ReplicaImpl.this.callbackPendingQueue.add(callback)) {
                    ThreadUtil.runCallback(callback, Finished.failure(new PacificaException(String.format("replica=%s failed to append callback", ReplicaImpl.this.replicaId))));
                }
            }
            //
            final long startLogIndex = this.getFirstLogIndex();
            final long endLogIndex = startLogIndex + appendCount - 1;
            ReplicaImpl.this.senderGroup.continueAppendLogEntry(endLogIndex);
            ReplicaImpl.this.ballotBox.ballotBy(replicaId, startLogIndex, endLogIndex);
            for (; index < this.failureCallbacks.size(); index++) {
                Callback callback = this.failureCallbacks.get(index);
                ThreadUtil.runCallback(callback, finished);
            }
        }
    }

    class SecondaryAppendLogEntriesCallback extends LogManager.AppendLogEntriesCallback {

        private final RpcResponseCallback<RpcRequest.AppendEntriesResponse> rpcCallback;


        public SecondaryAppendLogEntriesCallback(RpcResponseCallback<RpcRequest.AppendEntriesResponse> rpcCallback) {
            this.rpcCallback = rpcCallback;
        }

        @Override
        public void run(Finished finished) {
            if (!finished.isOk()) {
                ThreadUtil.runCallback(rpcCallback, finished);
                return;
            }
            //TODO  maybe commit at min(prev_log_index + append_count, request_commit_point)?
            final long lastLogIndex = this.getFirstLogIndex() + this.getAppendCount() - 1;
            final long curTerm = ReplicaImpl.this.replicaGroup.getPrimaryTerm();
            RpcRequest.AppendEntriesResponse response = RpcRequest.AppendEntriesResponse
                    .newBuilder()//
                    .setSuccess(true)//
                    .setTerm(curTerm)//
                    .setLastLogIndex(lastLogIndex)//
                    .build();
            rpcCallback.setRpcResponse(response);
            ThreadUtil.runCallback(rpcCallback, finished);
        }
    }

    /**
     * TODO recycle
     */
    class OperationConsumer implements Runnable {
        private List<OperationContext> buffer = new ArrayList<>(16);

        @Override
        public void run() {
            if (ReplicaImpl.this.operationContextQueue.isEmpty()) {
                return;
            }
            try {
                final Queue<OperationContext> queue = ReplicaImpl.this.operationContextQueue;
                final int maxCount = ReplicaImpl.this.option.getMaxOperationNumPerBatch();
                OperationContext operationContext;
                while (buffer.size() < maxCount && (operationContext = queue.poll()) != null) {
                    buffer.add(operationContext);
                }
                ReplicaImpl.this.applyOperationBatch(buffer);
            } finally {
                this.rest();
            }
        }

        void rest() {
            this.buffer.clear();
        }
    }

}
