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
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.*;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcResponseCallback;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.sender.SenderGroup;
import com.trs.pacifica.sender.SenderType;
import com.trs.pacifica.util.QueueUtil;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.TimeUtils;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
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

    private Queue<OperationContext> operationContextQueue = QueueUtil.newMpscQueue();

    private ReplicaState state = ReplicaState.Uninitialized;

    private ReplicaGroup replicaGroup;

    private ConfigurationClient configurationClient;

    private PacificaClient pacificaClient;

    private LogManagerImpl logManager;

    private SnapshotManagerImpl snapshotManager;

    private SenderGroup senderGroup;

    private BallotBoxImpl ballotBox;

    private ExecutorGroup executorGroup;

    private SingleThreadExecutor applyExecutor;

    /**
     * last timestamp receive heartbeat request from primary
     */
    private volatile long lastPrimaryVisit = TimeUtils.monotonicMs();

    public ReplicaImpl(ReplicaId replicaId) {
        this.replicaId = replicaId;
    }


    private void initLogManager(ReplicaOption option) {
        final PacificaServiceFactory pacificaServiceFactory = Objects.requireNonNull(option.getPacificaServiceFactory(), "pacificaServiceFactory");
        final String logStoragePath = Objects.requireNonNull(option.getLogStoragePath(), "logStoragePath");
        final LogStorage logStorage = pacificaServiceFactory.newLogStorage(logStoragePath);
        this.logManager = new LogManagerImpl();

    }

    private void initSnapshotManager(ReplicaOption option) {

    }

    private void initSenderGroup(ReplicaOption option) {
        this.senderGroup = new SenderGroupImpl(Objects.requireNonNull(this.pacificaClient, "pacificaClient"));
    }


    private void initExecutor(ReplicaOption option) {
        this.executorGroup = Objects.requireNonNull(option.getExecutorGroup(), "executorGroup");
        this.applyExecutor = Objects.requireNonNull(this.executorGroup.chooseExecutor());
    }

    private void initBallotBox() {

    }

    private void initStateMachineCall() {

    }

    @Override
    public void init(ReplicaOption option) {
        this.writeLock.lock();
        try {
            if (this.state == ReplicaState.Uninitialized) {
                this.option = Objects.requireNonNull(option, "require option");
                this.configurationClient = Objects.requireNonNull(option.getConfigurationClient(), "configurationClient");
                this.pacificaClient = Objects.requireNonNull(option.getPacificaClient(), "pacificaClient");
                initExecutor(option);
                initLogManager(option);
                initSnapshotManager(option);
                initSenderGroup(option);
                initBallotBox();
                initStateMachineCall();
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

    private void ensureActive() {
        final ReplicaState replicaState = this.state;
        if (!replicaState.isActive()) {
            throw new IllegalStateException(String.format("replica(%s) is not active, current state is %s.", this.replicaId, replicaState));
        }
    }

    @Override
    public boolean isPrimary(boolean block) {
        return false;
    }

    @Override
    public void apply(Operation operation) {
        Objects.requireNonNull(operation, "param: operation is null");
        ensureActive();
        final LogEntry logEntry = new LogEntry();
        logEntry.setLogData(operation.getLogData());
        final OperationContext context = new OperationContext(logEntry, operation.getOnFinish());
        if (this.operationContextQueue.offer(context)) {
            this.applyExecutor.execute(new OperationConsumer());
        }
    }


    @Override
    public void snapshot(Callback onFinish) {

    }

    @Override
    public void recover(Callback onFinish) {

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
                //TODO

                return null;
            }
            final ReplicaId toReplicaId = RpcUtil.toReplicaId(request.getTargetId());
            if (!this.replicaId.equals(toReplicaId)) {
                //TODO
                return null;
            }

            if (this.replicaGroup.getVersion() < request.getVersion()) {
                // TODO refresh replica group

            }
            if (this.replicaGroup.getPrimaryTerm() > request.getTerm()) {
                //TODO
                return null;
            }
            final ReplicaId fromReplicaId = RpcUtil.toReplicaId(request.getPrimaryId());
            if (!this.replicaGroup.getPrimary().equals(fromReplicaId)) {
                //TODO
                return null;
            }
            updateLastPrimaryVisit();
            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            final long localPrevLogTerm = this.logManager.getLogTermAt(prevLogIndex);
            if (prevLogTerm != localPrevLogTerm) {
                // TODO
                return null;
            }
            if (request.getLogMetaCount() == 0) {

                return null;
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
            List<LogEntry> logEntries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final OperationContext context = oneBatch.get(i);
                final Callback callback = context.callback;
                final LogEntry logEntry = context.logEntry;
                //initiate ballot to ballotBox
                if (!this.ballotBox.initiateBallot(this.replicaGroup)) {

                    continue;
                }


                logEntry.setType(LogEntry.Type.OP_DATA);
                logEntry.getLogId().setTerm(curPrimaryTerm);
                logEntries.add(logEntry);
            }
            //log manager append log
            this.logManager.appendLogEntries(logEntries, new PrimaryAppendLogEntriesCallback());
        } finally {
            this.writeLock.unlock();
        }

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
     *
     * @return
     */
    private boolean isCurrentPrimaryValid() {
        return isWithinGracePeriod(TimeUtils.monotonicMs());
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

        @Override
        public void run(Finished finished) {
            if (finished.isOk()) {
                //success
                final long startLogIndex = this.getFirstLogIndex();
                final long endLogIndex = startLogIndex + this.getAppendCount() - 1;
                ReplicaImpl.this.ballotBox.ballotBy(replicaId, startLogIndex, endLogIndex);
            } else {
                //failure

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

            if (finished.isOk()) {
                //success
                //1、set commit point

                //2、send response
                this.rpcCallback.setRpcResponse(null);
            } else {
                //failure
                ThreadUtil.runCallback(rpcCallback, finished);
            }
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
            final Queue<OperationContext> queue = ReplicaImpl.this.operationContextQueue;
            final int maxCount = ReplicaImpl.this.option.getMaxOperationNumPerBatch();
            OperationContext operationContext;
            while (buffer.size() < maxCount && (operationContext = queue.poll()) != null) {
                buffer.add(operationContext);
            }
            ReplicaImpl.this.applyOperationBatch(buffer);
            this.rest();
        }

        void rest() {
            this.buffer.clear();
        }
    }

}
