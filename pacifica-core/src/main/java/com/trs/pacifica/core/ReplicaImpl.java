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
import com.trs.pacifica.error.NotSupportedException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.fs.FileService;
import com.trs.pacifica.fs.FileServiceFactory;
import com.trs.pacifica.log.codec.LogEntryCodecFactory;
import com.trs.pacifica.model.*;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.ExecutorRequestFinished;
import com.trs.pacifica.rpc.ReplicaService;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.client.impl.DefaultPacificaClient;
import com.trs.pacifica.rpc.node.EndpointFactory;
import com.trs.pacifica.sender.Sender;
import com.trs.pacifica.sender.SenderGroupImpl;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final Queue<OperationContext> operationContextQueue = QueueUtil.newMpscQueue();

    private final PendingQueue<Callback> callbackPendingQueue = new PendingQueueImpl<>();

    private volatile ReplicaState state = ReplicaState.Uninitialized;

    private CacheReplicaGroup replicaGroup;

    private ConfigurationClient configurationClient;

    private RpcClient rpcClient;
    private EndpointFactory endpointFactory;
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

    private FileService fileService;

    /**
     * last timestamp receive heartbeat request from primary
     */
    private volatile long lastPrimaryVisit = TimeUtils.monotonicMs();

    public ReplicaImpl(ReplicaId replicaId) {
        this.replicaId = replicaId;
    }


    private void initLogManager(ReplicaOption option) throws PacificaException {
        final ExecutorGroup logExecutorGroup = Objects.requireNonNull(option.getLogManagerExecutorGroup(), "LogManagerExecutorGroup");
        final PacificaServiceFactory pacificaServiceFactory = Objects.requireNonNull(option.getPacificaServiceFactory(), "pacificaServiceFactory");
        final String logStoragePath = Objects.requireNonNull(option.getLogStoragePath(), "logStoragePath");
        final LogManagerImpl.Option logManagerOption = new LogManagerImpl.Option();
        final LogEntryCodecFactory logEntryCodecFactory = Objects.requireNonNull(option.getLogEntryCodecFactory(), "logEntryCodecFactory");
        logManagerOption.setReplicaOption(option);
        logManagerOption.setLogStoragePath(logStoragePath);
        logManagerOption.setLogStorageFactory(pacificaServiceFactory);
        logManagerOption.setLogManagerExecutor(logExecutorGroup.chooseExecutor());
        logManagerOption.setStateMachineCaller(this.stateMachineCaller);
        logManagerOption.setLogEntryCodecFactory(logEntryCodecFactory);
        this.logManager.init(logManagerOption);
    }

    private void initSnapshotManager(ReplicaOption option) throws PacificaException {
        final SnapshotManagerImpl.Option snapshotManagerOption = new SnapshotManagerImpl.Option();
        snapshotManagerOption.setReplicaOption(option);
        snapshotManagerOption.setStoragePath(option.getSnapshotStoragePath());
        snapshotManagerOption.setSnapshotStorageFactory(option.getPacificaServiceFactory());
        snapshotManagerOption.setStateMachineCaller(this.stateMachineCaller);
        snapshotManagerOption.setLogManager(this.logManager);
        snapshotManagerOption.setPacificaClient(this.pacificaClient);
        snapshotManagerOption.setDownloadSnapshotTimeoutMs(option.getDownloadSnapshotTimeoutMs());
        this.snapshotManager.init(snapshotManagerOption);
    }

    private void initSenderGroup(ReplicaOption option) throws PacificaException {
        final SenderGroupImpl.Option senderGroupOption = new SenderGroupImpl.Option();
        senderGroupOption.setLogManager(Objects.requireNonNull(this.logManager));
        senderGroupOption.setStateMachineCaller(Objects.requireNonNull(stateMachineCaller));
        senderGroupOption.setSenderExecutorGroup(Objects.requireNonNull(option.getSenderExecutorGroup()));
        senderGroupOption.setBallotBox(this.ballotBox);
        senderGroupOption.setReplicaGroup(this.replicaGroup);
        senderGroupOption.setFileService(this.fileService);
        senderGroupOption.setConfigurationClient(this.configurationClient);
        senderGroupOption.setLeasePeriodTimeOutMs(option.getLeasePeriodTimeoutMs());
        senderGroupOption.setHeartBeatFactor(option.getHeartBeatFactor());
        senderGroupOption.setTimerFactory(Objects.requireNonNull(option.getTimerFactory()));
        this.senderGroup.init(senderGroupOption);
    }


    private void initApplyExecutor(ReplicaOption option) throws PacificaException {
        this.executorGroup = Objects.requireNonNull(option.getApplyExecutorGroup(), "executorGroup");
        this.applyExecutor = Objects.requireNonNull(this.executorGroup.chooseExecutor());
    }

    private void initBallotBox(ReplicaOption option) throws PacificaException {
        final BallotBoxImpl.Option ballotBoxOption = new BallotBoxImpl.Option();
        ballotBoxOption.setFsmCaller(Objects.requireNonNull(this.stateMachineCaller));
        ballotBox.init(ballotBoxOption);
    }

    private void initStateMachineCall(ReplicaOption option) throws PacificaException {
        final StateMachine stateMachine = Objects.requireNonNull(option.getStateMachine(), "state machine");
        final StateMachineCallerImpl.Option fsmOption = new StateMachineCallerImpl.Option();
        fsmOption.setStateMachine(stateMachine);
        fsmOption.setLogManager(Objects.requireNonNull(this.logManager));
        final ExecutorGroup fsmExecutorGroup = Objects.requireNonNull(option.getFsmCallerExecutorGroup(), "fsm executor group");
        fsmOption.setExecutor(Objects.requireNonNull(fsmExecutorGroup.chooseExecutor(), "fsm executor"));
        fsmOption.setCallbackPendingQueue(this.callbackPendingQueue);
        this.stateMachineCaller.init(fsmOption);
    }

    private void initPacificaClient(ReplicaOption option) throws PacificaException {
        this.rpcClient = Objects.requireNonNull(option.getRpcClient(), "rpcClient");
        this.pacificaClient = new DefaultPacificaClient(this.rpcClient, this.endpointFactory);
    }

    private void initRepeatedTimers(ReplicaOption option) {
        this.gracePeriodTimer = new RepeatedTimer("Grace_Period_Timer_" + this.replicaId.getGroupName(), option.getGracePeriodTimeoutMs(),
                Objects.requireNonNull(option.getTimerFactory().newTimer())) {
            @Override
            protected void onTrigger() {
                handleGracePeriodTimeout();
            }
        };

        this.leasePeriodTimer = new RepeatedTimer("Lease_Period_Timer_" + this.replicaId.getGroupName(), option.getLeasePeriodTimeoutRatio(),
                Objects.requireNonNull(option.getTimerFactory().newTimer())) {
            @Override
            protected void onTrigger() {
                handleLeasePeriodTimeout();
            }
        };

        this.snapshotTimer = new RepeatedTimer("Snapshot_Timer_" + this.replicaId.getGroupName(), option.getSnapshotTimeoutMs(),
                Objects.requireNonNull(option.getTimerFactory().newTimer())) {
            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }
        };

        this.recoverTimer = new RepeatedTimer("Replica_Recover_Timer_" + this.replicaId.getGroupName(), option.getRecoverTimeoutMs(),
                Objects.requireNonNull(option.getTimerFactory().newTimer())) {
            @Override
            protected void onTrigger() {
                handleRecoverTimeout();
            }
        };


    }

    @Override
    public void init(ReplicaOption option) throws PacificaException {
        this.writeLock.lock();
        try {
            if (this.state == ReplicaState.Uninitialized) {
                this.option = Objects.requireNonNull(option, "require option");
                this.configurationClient = Objects.requireNonNull(option.getConfigurationClient(), "configurationClient");
                this.replicaGroup = new CacheReplicaGroup(() -> {
                    return this.configurationClient.getReplicaGroup(this.replicaId.getGroupName());
                });
                this.endpointFactory = Objects.requireNonNull(option.getEndpointFactory(), "nodeManager");
                final FileServiceFactory fileServiceFactory = Objects.requireNonNull(option.getFileServiceFactory());
                this.fileService = Objects.requireNonNull(fileServiceFactory.newFileService());
                this.logManager = new LogManagerImpl(this);
                this.snapshotManager = new SnapshotManagerImpl(this);
                this.stateMachineCaller = new StateMachineCallerImpl(this);
                this.senderGroup = new SenderGroupImpl(this, this.pacificaClient);
                this.ballotBox = new BallotBoxImpl(this);
                initPacificaClient(option);
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
    public void startup() throws PacificaException {
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
    public void shutdown() throws PacificaException {
        this.writeLock.lock();
        try {
            this.state = ReplicaState.Shutdown;
            stopRecoverTimer();
            stopSnapshotTimer();
            stopGracePeriodTimer();
            stopLeasePeriodTimer();

            this.senderGroup.shutdown();
            this.ballotBox.shutdown();
            this.stateMachineCaller.shutdown();
            this.logManager.shutdown();
            this.snapshotManager.shutdown();

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
    public boolean isPrimary(boolean block) {
        return getReplicaState(block) == ReplicaState.Primary;
    }

    @Override
    public void apply(Operation operation) {
        Objects.requireNonNull(operation, "param: operation is null");
        try {
            ensureActive();
            final LogEntry logEntry = new LogEntry(LogEntry.Type.OP_DATA);
            logEntry.setLogData(operation.getLogData());
            apply(logEntry, operation.getOnFinish());
        } catch (PacificaException e) {
            ThreadUtil.runCallback(operation.getOnFinish(), Finished.failure(e));
        }
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
        this.readLock.lock();
        try {
            return this.stateMachineCaller.getCommittedPont();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogId getSnapshotLogId() {
        this.readLock.lock();
        try {
            return this.snapshotManager.getLastSnapshotLodId();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogId getFirstLogId() {
        this.readLock.lock();
        try {
            return this.logManager.getFirstLogId();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogId getLastLogId() {
        this.readLock.lock();
        try {
            return this.logManager.getLastLogId();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public RpcRequest.AppendEntriesResponse handleAppendLogEntryRequest(RpcRequest.AppendEntriesRequest request, RpcRequestFinished<RpcRequest.AppendEntriesResponse> callback) throws PacificaException {
        // Secondary or Candidate received AppendEntriesRequest from Primary
        this.readLock.lock();
        try {
            ensureActive();
            final ReplicaId targetId = RpcUtil.toReplicaId(request.getTargetId());
            if (!this.replicaId.equals(targetId)) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, String.format("mismatched target id.expect=%s, actual=%s.", this.replicaId, targetId));
            }
            if (this.replicaGroup.getVersion() < request.getVersion()) {
                // async align version
                asyncAlignReplicaGroupVersion(request.getVersion());
            }
            final ReplicaId fromReplicaId = RpcUtil.toReplicaId(request.getPrimaryId());
            final ReplicaId primaryReplicaId = this.replicaGroup.getPrimary();
            if (!primaryReplicaId.equals(fromReplicaId)) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, String.format("mismatched primary id. expect=%s, actual=%s.", primaryReplicaId, fromReplicaId));
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
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public RpcRequest.ReplicaRecoverResponse handleReplicaRecoverRequest(RpcRequest.ReplicaRecoverRequest request, RpcRequestFinished<RpcRequest.ReplicaRecoverResponse> callback) throws PacificaException {
        //Primary received ReplicaRecoverRequest from Candidate
        this.readLock.lock();
        try {
            ensureActive();
            if (this.state != ReplicaState.Primary) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, "");
            }
            final ReplicaId primaryId = RpcUtil.toReplicaId(request.getPrimaryId());
            if (!this.replicaId.equals(primaryId)) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, "");
            }
            final long version = this.replicaGroup.getVersion();
            final long localTerm = this.replicaGroup.getPrimaryTerm();
            if (request.getTerm() != localTerm) {
                return RpcRequest.ReplicaRecoverResponse.newBuilder()//
                        .setSuccess(false)//
                        .setTerm(localTerm)//
                        .setVersion(version)//
                        .build();//
            }
            final ReplicaId recoverId = RpcUtil.toReplicaId(request.getRecoverId());
            // add log sender for recoverId
            this.senderGroup.addSenderTo(recoverId, SenderType.Candidate, true);
            // wait caught up
            final CandidateCaughtUpCallback onCaughtUp = new CandidateCaughtUpCallback(recoverId, callback);
            this.senderGroup.waitCaughtUp(recoverId, onCaughtUp, 1000);
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public RpcRequest.InstallSnapshotResponse handleInstallSnapshotRequest(RpcRequest.InstallSnapshotRequest request, RpcRequestFinished<RpcRequest.InstallSnapshotResponse> callback) throws PacificaException {
        // Secondary or Candidate receive InstallSnapshotRequest
        this.readLock.lock();
        try {
            ensureActive();
            if (this.snapshotManager == null) {
                throw new PacificaException(PacificaErrorCode.NOT_SUPPORT, "not support install snapshot.");
            }
            final ReplicaId targetId = RpcUtil.toReplicaId(request.getTargetId());
            if (!this.replicaId.equals(targetId)) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, String.format("mismatched target id.expect=%s, actual=%s.", this.replicaId, targetId));
            }
            if (this.replicaGroup.getVersion() < request.getVersion()) {
                asyncAlignReplicaGroupVersion(request.getVersion());
            }
            final ReplicaId fromReplicaId = RpcUtil.toReplicaId(request.getPrimaryId());
            final ReplicaId primaryReplicaId = this.replicaGroup.getPrimary();
            if (!primaryReplicaId.equals(fromReplicaId)) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, String.format("mismatched primary id. expect=%s, actual=%s.", primaryReplicaId, fromReplicaId));
            }
            final long primaryTerm = this.replicaGroup.getPrimaryTerm();
            if (primaryTerm > request.getTerm()) {
                return RpcRequest.InstallSnapshotResponse.newBuilder()//
                        .setSuccess(false)//
                        .setTerm(primaryTerm)//
                        .build();
            }
            final SnapshotManager.InstallSnapshotCallback installSnapshotCallback = new SnapshotManager.InstallSnapshotCallback() {

                @Override
                public void run(Finished finished) {
                    if (finished.isOk()) {
                        final RpcRequest.InstallSnapshotResponse response = getResponse();
                        callback.setRpcResponse(response);
                    }
                    ThreadUtil.runCallback(callback, finished);
                }
            };
            return this.snapshotManager.installSnapshot(request, installSnapshotCallback);
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public RpcRequest.GetFileResponse handleGetFileRequest(RpcRequest.GetFileRequest request, RpcRequestFinished<RpcRequest.GetFileResponse> callback) throws PacificaException {
        this.readLock.lock();
        try {
            ensureActive();
            return fileService.handleGetFileRequest(request, callback);
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        } finally {
            this.readLock.unlock();
        }
        return null;
    }


    /**
     * apply batch operation only by primary
     *
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

    /**
     * Called when the state of the replica has changed.
     *
     * @throws PacificaException
     */
    private void onReplicaStateChange() throws PacificaException {
        ReplicaState oldState, newState;
        this.readLock.lock();
        try {
            oldState = this.state;
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
    private void becomePrimary() throws PacificaException {
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

    private void becomeSecondary() throws PacificaException {
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

    private void becomeCandidate() throws PacificaException {
        this.writeLock.lock();
        try {
            this.state = ReplicaState.Candidate;
            unsafeStepDown();
            startRecoverTimer();
            LOGGER.info("The replica({}) has become Candidate.", this.replicaId);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void apply(final LogEntry logEntry, final Callback onFinish) throws PacificaException {
        final OperationContext context = new OperationContext(logEntry, onFinish);
        if (this.operationContextQueue.offer(context)) {
            this.applyExecutor.execute(new OperationConsumer());
        } else {
            //TODO Rejection strategy
            // Currently throwing an PacificaException
            throw new PacificaException(PacificaErrorCode.BUSY, "operation queue is overload.");
        }
    }

    /**
     * call by Primary for reconciliation
     * should be in write lock
     */
    private void reconciliation() throws PacificaException {
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

    private void startSenderGroup() throws PacificaException {
        this.senderGroup.startup();
        List<ReplicaId> secondaries = this.replicaGroup.listSecondary();
        for (ReplicaId secondary : secondaries) {
            this.senderGroup.addSenderTo(secondary);
        }
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
     * @param monotonicNowMs
     * @return
     */
    private boolean isWithinGracePeriod(long monotonicNowMs) {
        return monotonicNowMs - this.lastPrimaryVisit < this.option.getGracePeriodTimeoutMs();
    }

    /**
     * Whether the current Primary is valid
     *
     * @return true if is valid
     */
    private boolean isCurrentPrimaryValid() {
        return isWithinGracePeriod(TimeUtils.monotonicMs());
    }


    /**
     * The Secondary checks whether the Primary is faulty.
     * If the Primary is faulty, change Primary.
     */
    private void handleGracePeriodTimeout() {
        long currentVersion = Long.MIN_VALUE;
        this.readLock.lock();
        try {
            if (this.state != ReplicaState.Secondary) {
                return;
            }
            if (isCurrentPrimaryValid()) {
                return;
            }
            currentVersion = this.replicaGroup.getVersion();
            LOGGER.info("The faulty Primary({}) was found. we will elect Primary. cur_version={}", this.replicaGroup.getPrimary(), currentVersion);
        } finally {
            this.readLock.unlock();
        }
        try {
            handleChangePrimary(currentVersion);
        } catch (PacificaException e) {
            LOGGER.error("{} failed to change Primary on grace period timeout.", this.replicaId, e);
        }

    }

    /**
     * the current Primary is faulty, will change Primary.
     *
     * @param currentVersion The version number of the Primary at the time of faulty
     */
    private void handleChangePrimary(final long currentVersion) throws PacificaException {
        if (maybeRefreshReplicaGroup(currentVersion + 1)) {
            // It is possible to be preempted by other Secondary,
            // so we try to update the state of the current Replica.
            onReplicaStateChange();
        } else {
            // elect self as the Primary
            electSelf(currentVersion);
        }
    }

    private void electSelf(final long version) throws PacificaException {
        if (this.configurationClient.changePrimary(version, this.replicaId)) {
            // success to elect self as the Primary
            refreshReplicaGroupUtil(version + 1);
        }
    }

    void refreshReplicaGroupUtil(long requestVersion) throws PacificaException {
        boolean failure = false;
        do {
            this.writeLock.lock();
            try {
                failure = !alignReplicaGroupVersion(requestVersion);
            } finally {
                this.writeLock.unlock();
            }
            if (failure) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn("{} refresh ReplicaGroup Util version={}. we will retry.", this.replicaId,
                            requestVersion);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        } while (failure);
    }

    /**
     * the Replica Group align to higher version number.
     *
     * @param higherVersion
     * @return true if success align
     */
    private boolean alignReplicaGroupVersion(final long higherVersion) throws PacificaException {
        long currentVersion = Long.MIN_VALUE;
        this.readLock.lock();
        try {
            currentVersion = this.replicaGroup.getVersion();
            if (currentVersion >= higherVersion) {
                return true;
            }
            LOGGER.info("{} try align to higher_version={}, cur_version={}, we try refresh replica_group.", this.replicaId, higherVersion, currentVersion);
            currentVersion = forceRefreshReplicaGroup();
        } finally {
            this.readLock.unlock();
        }
        if (currentVersion >= higherVersion) {
            onReplicaStateChange();
            return true;
        } else {
            LOGGER.info("{} try align to higher_version={}, cur_version={}, but failed to obtain the latest version number", this.replicaId, higherVersion, currentVersion);
            return false;
        }
    }

    private void asyncAlignReplicaGroupVersion(final long higherVersion) {
        this.applyExecutor.execute(
                () -> {
                    try {
                        alignReplicaGroupVersion(higherVersion);
                    } catch (Throwable e) {
                    }
                }
        );
    }

    private boolean maybeRefreshReplicaGroup(long version) {
        long currentVersion;
        this.readLock.lock();
        try {
            currentVersion = this.replicaGroup.getVersion();
            if (version > currentVersion) {
                forceRefreshReplicaGroup();
            }
        } finally {
            this.readLock.unlock();
        }
        if (this.replicaGroup.getVersion() <= currentVersion) {
            return false;
        }
        LOGGER.info("the replica({}) has changed. new replica group={}", this, this.replicaGroup);
        return true;
    }

    /**
     * The Primary checks whether the Secondary is faulty.
     * If the Secondary is faulty, remove Secondary.
     */
    private void handleLeasePeriodTimeout() {
        long currentVersion;
        List<ReplicaId> removed = null;
        this.readLock.lock();
        try {
            if (this.state != ReplicaState.Primary) {
                return;
            }
            final List<ReplicaId> secondaries = this.replicaGroup.listSecondary();
            if (secondaries == null || secondaries.isEmpty()) {
                return;
            }
            removed = new ArrayList<>(secondaries.size());
            currentVersion = replicaGroup.getVersion();
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
                    LOGGER.warn("Response was received for failure removing the Secondary({}) from the config cluster, current_version={}", secondary, currentVersion);
                    break;
                }

            }
        } finally {
            this.readLock.unlock();
        }
        if (removed != null && !removed.isEmpty()) {
            for (ReplicaId removedSecondary : removed) {
                // 1.remove sender
                this.senderGroup.removeSender(removedSecondary);
                // 2.abandon ballot
                this.ballotBox.cancelBallot(removedSecondary);
                LOGGER.info("{} is faulty, success to remove.", removedSecondary);
            }
        }
        asyncAlignReplicaGroupVersion(currentVersion);

    }

    /**
     *
     */
    private void handleSnapshotTimeout() {
        this.readLock.lock();
        try {
            if (!this.state.isActive()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn("The replica={} not active on handle snapshot timeout, state={}", this.replicaId, this.state);
                }
                return;
            }
            ThreadUtil.runInThread(() -> {
                doSnapshot(null);
            });
        } finally {
            this.readLock.unlock();
        }

    }

    private void handleRecoverTimeout() {
        this.readLock.lock();
        try {
            if (this.state != ReplicaState.Candidate) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn("The replica={} not Candidate on handle recover timeout, state={}", this.replicaId, this.state);
                }
                return;
            }
            if (this.recovering.get()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.warn("The replica={} is recovering.", this.replicaId);
                }
                return;
            }
            ThreadUtil.runInThread(() -> {
                doRecover(null);
            });
        } finally {
            this.readLock.unlock();
        }
    }

    boolean isAlive(final ReplicaId secondary) {
        Objects.requireNonNull(secondary, "secondary");
        return this.senderGroup.isAlive(secondary);
    }

    void ensureActive() throws PacificaException {
        final ReplicaState replicaState = this.state;
        if (!replicaState.isActive()) {
            throw new PacificaException(PacificaErrorCode.UNAVAILABLE, String.format("Current replica state=%s is not active", replicaState));
        }
    }

    private void doSnapshot(final Callback onFinish) {
        this.readLock.lock();
        try {
            ensureActive();
            if (this.snapshotManager != null) {
                this.snapshotManager.doSnapshot(onFinish);
            } else {
                throw new NotSupportedException("Snapshot is not supported");
            }
        } catch (Throwable e) {
            ThreadUtil.runCallback(onFinish, Finished.failure(e));
        } finally {
            this.readLock.unlock();
        }
    }

    private void doRecover(final Callback onFinish) {
        try {
            ensureActive();
            if (!this.recovering.compareAndSet(false, true)) {
                throw new PacificaException(PacificaErrorCode.BUSY, "The Candidate busy with recovery.");
            }
            if (this.state != ReplicaState.Candidate) {
                throw new PacificaException(PacificaErrorCode.NOT_SUPPORT, "Only Candidate state needs to recover. current state is " + this.state);
            }
            this.readLock.lock();
            try {
                final ReplicaId primaryId = this.replicaGroup.getPrimary();
                final long term = this.replicaGroup.getPrimaryTerm();
                RpcRequest.ReplicaRecoverRequest recoverRequest = RpcRequest.ReplicaRecoverRequest.newBuilder()//
                        .setPrimaryId(RpcUtil.protoReplicaId(primaryId))//
                        .setRecoverId(RpcUtil.protoReplicaId(this.replicaId))//
                        .setTerm(term)//
                        .build();
                this.pacificaClient.recoverReplica(recoverRequest, new ExecutorRequestFinished<RpcRequest.ReplicaRecoverResponse>() {
                    @Override
                    protected void doRun(Finished finished) {
                        handleReplicaRecoverResponse(finished, getRpcResponse(), onFinish);
                    }
                });
            } catch (Throwable e) {
                this.recovering.set(false);
                ThreadUtil.runCallback(onFinish, Finished.failure(e));
            } finally {
                this.readLock.unlock();
            }
        } catch (Throwable e) {
            ThreadUtil.runCallback(onFinish, Finished.failure(e));
        }
    }

    private void handleReplicaRecoverResponse(final Finished finished, final RpcRequest.ReplicaRecoverResponse response, final @Nullable Callback onFinish) {
        try {
            if (!finished.isOk()) {
                ThreadUtil.runCallback(onFinish, finished);
                return;
            }
            assert response != null;
            if (!response.getSuccess()) {
                // the local term is lower
                alignReplicaGroupVersion(response.getVersion());
                ThreadUtil.runCallback(onFinish, Finished.failure(new PacificaException(PacificaErrorCode.NO_MATCH_TERM, "Received a response that the Primary failed to recover")));
                return;
            }
            refreshReplicaGroupUtil(response.getVersion());
            ThreadUtil.runCallback(onFinish, Finished.success());
            LOGGER.info("{} success to recover.", this.replicaId);
        } catch (PacificaException e) {
            ThreadUtil.runCallback(onFinish, Finished.failure(e));
            LOGGER.error("{} failed to recover.", this.replicaId);
        } finally {
            this.recovering.set(false);
        }
    }

    private long forceRefreshReplicaGroup() {
        this.replicaGroup.clearCache();
        return this.replicaGroup.getVersion();
    }

    /**
     * on receive primary term
     *
     * @param higherTerm
     */
    public boolean onReceiveHigherTerm(final long higherTerm) {
        long curVersion = Long.MIN_VALUE;
        this.readLock.lock();
        try {
            curVersion = this.replicaGroup.getVersion();
            final long curTerm = this.replicaGroup.getPrimaryTerm();
            if (higherTerm <= curTerm) {
                return false;
            }
            LOGGER.warn("{} receive higher term. request_term={}, cur_term={}", this.replicaId, higherTerm, curTerm);
        } finally {
            this.readLock.unlock();
        }
        try {
            return this.alignReplicaGroupVersion(curVersion + 1);
        } catch (PacificaException e) {
            LOGGER.error("{} failed to align replica group. ", this.replicaId);
        }
        return false;
    }

    /**
     * on PacificaException occurs
     *
     * @param pacificaException
     */
    public void onError(PacificaException pacificaException) {
        this.writeLock.lock();
        try {
            if (this.state == ReplicaState.Error) {
                LOGGER.error("{} encountered an error. but it is repeat.", this.replicaId, pacificaException);
                return;
            }
            LOGGER.error("{} encountered an error.", this.replicaId, pacificaException);
            //1. state machine on error
            if (this.stateMachineCaller != null) {
                this.stateMachineCaller.onError(pacificaException);
            }
            //2. enter error state
            //Primary Secondary
            unsafeBecomeErrorState();

        } finally {
            this.writeLock.unlock();
        }

    }

    private void unsafeBecomeErrorState() {
        if (this.state.compareTo(ReplicaState.Candidate) < 0) {
            // Primary  Secondary step down
            unsafeStepDown();
            //
        }

        this.state = ReplicaState.Error;
    }

    /**
     * Primary 、Secondary step down to Candidate
     * close something
     */
    private void unsafeStepDown() {
        // case 1: Primary -> Candidate
        // case 2: Secondary -> Candidate
        this.ballotBox.shutdown();
        this.senderGroup.shutdown();
        stopLeasePeriodTimer();
        stopGracePeriodTimer();
        stopSnapshotTimer();
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

    class CandidateCaughtUpCallback extends Sender.OnCaughtUp {

        private final ReplicaId recoverId;
        private final RpcRequestFinished<RpcRequest.ReplicaRecoverResponse> callback;

        CandidateCaughtUpCallback(ReplicaId recoverId, RpcRequestFinished<RpcRequest.ReplicaRecoverResponse> callback) {
            this.recoverId = recoverId;
            this.callback = callback;
        }

        @Override
        public void run(Finished finished) {
            if (!finished.isOk()) {
                ThreadUtil.runCallback(callback, finished);
                LOGGER.error("Failed to recover. replica_id={}.", recoverId, finished.error());
                return;
            }
            //add secondary

            //inc ballot


        }
    }

    class PrimaryAppendLogEntriesCallback extends LogManager.AppendLogEntriesCallback {

        private final List<Callback> callbacks;

        PrimaryAppendLogEntriesCallback(List<Callback> callbacks) {
            this.callbacks = callbacks;
        }


        @Override
        public void run(final Finished finished) {
            // When you've reached this point,
            // it means that the Primary has persisted the op-log, or failure
            final long startLogIndex = this.getFirstLogIndex();
            int appendCount = this.getAppendCount();
            int index = 0;
            for (; index < appendCount; index++) {
                final Callback callback = this.callbacks.get(index);
                //initiate ballot to ballotBox
                if (!ReplicaImpl.this.ballotBox.initiateBallot(startLogIndex + index, ReplicaImpl.this.replicaGroup)) {
                    ThreadUtil.runCallback(callback, Finished.failure(new PacificaException(PacificaErrorCode.INTERNAL, String.format("replica=%s failed to initiate ballot", ReplicaImpl.this.replicaId))));
                    continue;
                }
                if (!ReplicaImpl.this.callbackPendingQueue.add(callback)) {
                    ThreadUtil.runCallback(callback, Finished.failure(new PacificaException(PacificaErrorCode.INTERNAL, String.format("replica=%s failed to append callback", ReplicaImpl.this.replicaId))));
                }
            }
            //
            final long endLogIndex = startLogIndex + appendCount - 1;
            ReplicaImpl.this.senderGroup.continueAppendLogEntry(endLogIndex);
            ReplicaImpl.this.ballotBox.ballotBy(replicaId, startLogIndex, endLogIndex);
            for (; index < this.callbacks.size(); index++) {
                Callback callback = this.callbacks.get(index);
                ThreadUtil.runCallback(callback, finished);
            }
        }
    }

    class SecondaryAppendLogEntriesCallback extends LogManager.AppendLogEntriesCallback {

        private final RpcRequestFinished<RpcRequest.AppendEntriesResponse> rpcCallback;


        public SecondaryAppendLogEntriesCallback(RpcRequestFinished<RpcRequest.AppendEntriesResponse> rpcCallback) {
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
