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
import com.trs.pacifica.async.FinishedImpl;
import com.trs.pacifica.async.thread.ExecutorGroup;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.Operation;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcResponseCallback;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.sender.SenderGroup;
import com.trs.pacifica.sender.SenderType;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
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

    private ReplicaState state = ReplicaState.Uninitialized;

    private ConfigurationClient configurationClient;

    private PacificaClient pacificaClient;

    private LogManagerImpl logManager;

    private SnapshotManagerImpl snapshotManager;

    private SenderGroup senderGroup;

    private ExecutorGroup executorGroup;

    private SingleThreadExecutor applyExecutor;

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

        final OperationContext context = new OperationContext(null);


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

    private void doOperation(OperationContext operationContext) {

        this.writeLock.lock();
        try {
            final Callback callback = operationContext.callback;
            //check primary state
            if (this.state != ReplicaState.Primary) {
                ThreadUtil.runCallback(callback, FinishedImpl.failure(new IllegalStateException("Is not Primary.")));
                return;
            }
            //initiate ballot to ballotBox

            //log manager append log


        } finally {
            this.writeLock.unlock();
        }

    }

    /**
     * TODO recycle
     */
    static class OperationContext {

        LogEntry logEntry;

        Callback callback = null;

        int expectedTerm = -1;

        public OperationContext(LogEntry logEntry) {
            this.logEntry = logEntry;
        }


    }

    static class OperationHandler {

    }

}
