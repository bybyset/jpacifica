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
import com.trs.pacifica.async.DirectExecutor;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.SnapshotDownloader;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.IOUtils;
import com.trs.pacifica.util.RpcLogUtil;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SnapshotManagerImpl implements SnapshotManager, LifeCycle<SnapshotManagerImpl.Option> {

    static final Logger LOGGER = LoggerFactory.getLogger(SnapshotManagerImpl.class);

    private static final AtomicReferenceFieldUpdater<SnapshotManagerImpl, State> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(SnapshotManagerImpl.class, State.class, "state");
    private volatile State state = State.UNINITIALIZED;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final ReplicaImpl replica;
    private Option option;
    private SnapshotStorage snapshotStorage;
    private LogManager logManager;
    private StateMachineCaller stateMachineCaller;
    private LogId lastSnapshotLogId = new LogId(0, 0);
    private PacificaClient pacificaClient;
    private SnapshotDownloader snapshotDownloader = null;

    private SnapshotStorageFactory snapshotStorageFactory = null;


    public SnapshotManagerImpl(final ReplicaImpl replica) {
        this.replica = replica;
    }

    @Override
    public void init(Option option) throws PacificaException {
        this.writeLock.lock();
        try {
            if (this.state == State.UNINITIALIZED) {
                this.option = Objects.requireNonNull(option, "option");
                this.snapshotStorageFactory = Objects.requireNonNull(option.getSnapshotStorageFactory(), "snapshot storage factory");
                this.logManager = Objects.requireNonNull(option.getLogManager(), "logManager");
                this.stateMachineCaller = Objects.requireNonNull(option.getStateMachineCaller(), "stateMachineCaller");
                this.pacificaClient = Objects.requireNonNull(option.getPacificaClient(), "pacificaClient");
                this.state = State.SHUTDOWN;
            }
        } finally {
            this.writeLock.unlock();
        }

    }

    @Override
    public void startup() throws PacificaException {
        this.writeLock.lock();
        try {
            if (this.state == State.SHUTDOWN) {
                final String storagePath = Objects.requireNonNull(option.getStoragePath(), "storage path");
                this.snapshotStorage = this.snapshotStorageFactory.newSnapshotStorage(storagePath);
                this.state = State.IDLE;
                doFirstSnapshotLoad();
            }
        } finally {
            this.writeLock.unlock();
        }

    }

    @Override
    public void shutdown() throws PacificaException {
        this.writeLock.lock();
        try {
            if (this.state != State.SHUTDOWN) {
                this.state = State.SHUTTING;

                if (snapshotDownloader != null) {
                    snapshotDownloader.cancel();
                }
                this.state = State.SHUTDOWN;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public RpcRequest.InstallSnapshotResponse installSnapshot(RpcRequest.InstallSnapshotRequest installSnapshotRequest, final InstallSnapshotCallback callback) {
        try {
            ensureStarted();
            LogId installLogId = null;
            this.readLock.lock();
            try {
                final long snapshotLogIndex = installSnapshotRequest.getSnapshotLogIndex();
                final long snapshotLogTerm = installSnapshotRequest.getSnapshotLogTerm();
                installLogId = new LogId(snapshotLogIndex, snapshotLogTerm);
                if (installLogId.compareTo(this.lastSnapshotLogId) < 0) {
                    //installed
                    LOGGER.warn("{} receive InstallSnapshotRequest({}), but has been installed. local_snapshot_log_id={}",
                            replica.getReplicaId(), RpcLogUtil.toLogString(installSnapshotRequest), this.lastSnapshotLogId);
                    throw new PacificaException(PacificaErrorCode.INTERNAL, "has been installed.");
                }
            } finally {
                this.readLock.unlock();
            }
            if (STATE_UPDATER.compareAndSet(this, State.IDLE, State.SNAPSHOT_DOWNLOADING)) {
                try {
                    final ReplicaId primaryId = RpcUtil.toReplicaId(installSnapshotRequest.getPrimaryId());
                    //1、download snapshot from Primary
                    LOGGER.info("{} download snapshot from Primary={}", this.replica.getReplicaId(), primaryId);
                    final SnapshotStorage.DownloadContext context = new SnapshotStorage.DownloadContext(installLogId, installSnapshotRequest.getReaderId(), this.pacificaClient, primaryId);
                    int downloadSnapshotTimeoutMs = this.option.getDownloadSnapshotTimeoutMs();
                    if (downloadSnapshotTimeoutMs > 0) {
                        context.setTimeoutMs(downloadSnapshotTimeoutMs);
                    }
                    Executor downloadExecutor = this.option.getReplicaOption().getDownloadSnapshotExecutor();
                    if (downloadExecutor == null) {
                        downloadExecutor = new DirectExecutor();
                    }
                    context.setDownloadExecutor(downloadExecutor);
                    try (final SnapshotDownloader snapshotDownloader = this.snapshotStorage.startDownloadSnapshot(context)) {
                        this.snapshotDownloader = snapshotDownloader;
                        snapshotDownloader.start();
                        snapshotDownloader.awaitComplete();
                    } catch (ExecutionException e) {
                        throw new PacificaException(PacificaErrorCode.USER_ERROR, String.format("failed to download snapshot from Primary(%s)", primaryId), e.getCause());
                    } catch (InterruptedException e) {
                        throw new PacificaException(PacificaErrorCode.INTERRUPTED, String.format("%s interrupted download snapshot.", this.replica.getReplicaId()), e);
                    } finally {
                        snapshotDownloader = null;
                    }
                    //2、load snapshot
                    final SnapshotReader snapshotReader = this.snapshotStorage.openSnapshotReader();
                    if (snapshotReader == null) {
                        throw new PacificaException(PacificaErrorCode.INTERNAL, "failed to open snapshot reader after download snapshot.");
                    }
                    final InstalledSnapshotLoadCallback installedSnapshotLoadCallback = new InstalledSnapshotLoadCallback(callback, snapshotReader);
                    doSnapshotLoad(installedSnapshotLoadCallback, State.SNAPSHOT_DOWNLOADING);
                } catch (Throwable e) {
                    STATE_UPDATER.set(this, State.IDLE);
                    ThreadUtil.runCallback(callback, Finished.failure(e));
                }
            } else {
                throw new PacificaException(PacificaErrorCode.BUSY, String.format("the snapshot manager is busy doing %s.", this.state));
            }
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        }
        return null;
    }

    private boolean isStarted() {
        return this.state.compareTo(State.UNINITIALIZED) > 0;
    }

    private void ensureStarted() {
        if (!isStarted()) {
            throw new AlreadyClosedException(String.format("%s is not started.", this.getClass().getSimpleName()));
        }
    }

    @Override
    public void doSnapshot(final Callback callback) {
        try {
            ensureStarted();
            final int snapshotLogIndexMargin = Math.max(0, this.option.getReplicaOption().getSnapshotLogIndexMargin());
            if (STATE_UPDATER.compareAndSet(this, State.IDLE, State.SNAPSHOT_SAVING)) {
                this.readLock.lock();
                try {
                    final long lastAppliedLogIndex = this.stateMachineCaller.getLastAppliedLogIndex();
                    if (lastAppliedLogIndex < this.lastSnapshotLogId.getIndex()) {
                        // Our program should not go into this code block.
                        // TODO This error should probably be reported to the replica?
                        throw new PacificaException(PacificaErrorCode.INTERNAL, String.format("committed_log_index=%d less than snapshot_log_index=%d", lastAppliedLogIndex, this.lastSnapshotLogId.getIndex()));
                    }
                    if (lastAppliedLogIndex == this.lastSnapshotLogId.getIndex()) {
                        // ok, no need to execute.
                        ThreadUtil.runCallback(callback, Finished.success());
                        return;
                    }
                    final long distance = lastAppliedLogIndex - this.lastSnapshotLogId.getIndex();
                    if (distance < snapshotLogIndexMargin) {
                        // Set by the user, how many logs of operations will be kept without taking snapshot.
                        // snapshotLogIndexMargin
                        throw new PacificaException(PacificaErrorCode.UNAVAILABLE, String.format("You set snapshotLogIndexMargin=%d, " +
                                "so skip it. committed_log_index=%s, last_snapshot_log_index=%d", snapshotLogIndexMargin, lastAppliedLogIndex, this.lastSnapshotLogId.getIndex()));
                    }
                    final SnapshotSaveCallback snapshotSaveCallback = new SnapshotSaveCallback(callback);
                    this.stateMachineCaller.onSnapshotSave(snapshotSaveCallback);
                    LOGGER.debug("{} do snapshot save", this.replica.getReplicaId());
                } finally {
                    this.readLock.unlock();
                }
            } else {
                throw new PacificaException(PacificaErrorCode.BUSY, "it is busy or not started. state=" + this.state);
            }
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        }
    }


    @Override
    public LogId getLastSnapshotLodId() {
        this.readLock.lock();
        try {
            return this.lastSnapshotLogId.copy();
        } finally {
            this.readLock.unlock();
        }
    }

    private void doSnapshotLoad(final StateMachineCaller.SnapshotLoadCallback snapshotLoadCallback, final State expectState) throws PacificaException {
        if (STATE_UPDATER.compareAndSet(this, expectState, State.SNAPSHOT_LOADING)) {
            try {
                if (this.stateMachineCaller.onSnapshotLoad(snapshotLoadCallback)) {
                    try {
                        snapshotLoadCallback.awaitComplete();
                    } catch (InterruptedException e) {
                        throw new PacificaException(PacificaErrorCode.INTERRUPTED, String.format("%s interrupted load snapshot.", this.replica.getReplicaId()));
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        throw new PacificaException(PacificaErrorCode.USER_ERROR, String.format("%s failed to load snapshot, msg=%s", this.replica.getReplicaId(), cause.getCause()), cause);
                    }
                } else {
                    throw new PacificaException(PacificaErrorCode.BUSY, String.format("%s is busy do other task", this.replica.getReplicaId()));
                }
            } finally {
                STATE_UPDATER.set(this, State.IDLE);
            }
        } else {
            throw new PacificaException(PacificaErrorCode.BUSY, String.format("%s is busy load snapshot, do not repeat", this.replica.getReplicaId()));
        }
    }


    private void doFirstSnapshotLoad() throws PacificaException {
        try {
            final SnapshotReader snapshotReader = this.snapshotStorage.openSnapshotReader();
            if (snapshotReader == null) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, "can not open SnapshotReader");
            }
            final FirstSnapshotLoadCallback firstSnapshotLoadCallback = new FirstSnapshotLoadCallback(snapshotReader);
            doSnapshotLoad(firstSnapshotLoadCallback, State.IDLE);
        } catch (IOException e) {
            throw new PacificaException(PacificaErrorCode.IO, "Failed to do first snapshot, msg=" + e.getMessage(), e);
        }

    }

    private void onSnapshotLoadSuccess(final LogId loadSnapshotLogId) {
        assert loadSnapshotLogId != null;
        this.writeLock.lock();
        try {
            final long snapshotLogIndex = loadSnapshotLogId.getIndex();
            final long snapshotLogTerm = loadSnapshotLogId.getTerm();
            final LogId snapshotLogId = loadSnapshotLogId.copy();
            if (snapshotLogId.compareTo(this.lastSnapshotLogId) < 0) {
                LOGGER.warn("{} success to load snapshot, but cur_snapshot_log_id less than last_snapshot_log_id", this.replica.getReplicaId(), snapshotLogId, lastSnapshotLogId);
            }
            this.lastSnapshotLogId = snapshotLogId;
            this.logManager.onSnapshot(snapshotLogIndex, snapshotLogTerm);
            LOGGER.info("{} success to load snapshot, snapshot_log_id={}", this.replica.getReplicaId(), snapshotLogId);
        } finally {
            this.writeLock.unlock();
        }

    }

    private void onSnapshotSaveSuccess(final LogId saveLogId) {
        //
        assert saveLogId != null;
        this.writeLock.lock();
        try {
            if (saveLogId.compareTo(this.lastSnapshotLogId) > 0) {
                this.lastSnapshotLogId = saveLogId.copy();
                this.logManager.onSnapshot(saveLogId.getIndex(), saveLogId.getTerm());
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        return "SnapshotManagerImpl{" +
                "state=" + state +
                ", lastSnapshotLogId=" + this.lastSnapshotLogId +
                '}';
    }

    class SnapshotSaveCallback implements StateMachineCaller.SnapshotSaveCallback {

        private final Callback callback;

        private SnapshotWriter snapshotWriter = null;
        private LogId saveLogId = null;

        SnapshotSaveCallback(Callback callback) {
            this.callback = callback;
        }

        @Override
        public void run(Finished finished) {
            try {
                Objects.requireNonNull(this.saveLogId, "save snapshot LogId");
                if (this.snapshotWriter != null) {
                    this.snapshotWriter.close();
                }
                if (finished.isOk()) {
                    onSnapshotSaveSuccess(this.saveLogId);
                }
                ThreadUtil.runCallback(callback, finished);
            } catch (Throwable throwable) {
                if (!finished.isOk()) {
                    throwable.addSuppressed(finished.error());
                }
                ThreadUtil.runCallback(callback, Finished.failure(throwable));
            } finally {
                STATE_UPDATER.compareAndSet(SnapshotManagerImpl.this, State.SNAPSHOT_SAVING, State.IDLE);
            }
        }

        @Override
        public SnapshotWriter start(LogId saveLogId) throws IOException {
            this.saveLogId = saveLogId;
            this.snapshotWriter = SnapshotManagerImpl.this.snapshotStorage.openSnapshotWriter(saveLogId);
            return this.snapshotWriter;
        }

        @Override
        public LogId getSaveLogId() {
            if (this.saveLogId != null) {
                return saveLogId.copy();
            }
            return null;
        }
    }


    class FirstSnapshotLoadCallback implements StateMachineCaller.SnapshotLoadCallback {

        private volatile Throwable error = null;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final SnapshotReader snapshotReader;

        FirstSnapshotLoadCallback(final SnapshotReader snapshotReader) {
            this.snapshotReader = snapshotReader;
        }

        @Override
        public SnapshotReader getSnapshotReader() {
            return this.snapshotReader;
        }

        @Override
        public void run(Finished finished) {
            try {
                if (finished.isOk()) {
                    onSnapshotLoadSuccess(snapshotReader.getSnapshotLogId());
                } else {
                    this.error = finished.error();
                }
            } finally {
                this.latch.countDown();
                try {
                    IOUtils.close(this.snapshotReader);
                } catch (IOException e) {
                }
            }
        }

        public void awaitComplete() throws InterruptedException, ExecutionException {
            this.latch.await();
            if (this.error != null) {
                throw new ExecutionException(this.error);
            }
        }
    }

    class InstalledSnapshotLoadCallback implements StateMachineCaller.SnapshotLoadCallback {

        private final InstallSnapshotCallback installSnapshotCallback;
        private final SnapshotReader snapshotReader;

        InstalledSnapshotLoadCallback(InstallSnapshotCallback installSnapshotCallback, SnapshotReader snapshotReader) {
            this.installSnapshotCallback = installSnapshotCallback;
            this.snapshotReader = snapshotReader;
        }

        @Override
        public SnapshotReader getSnapshotReader() {
            return snapshotReader;
        }

        @Override
        public void awaitComplete() throws InterruptedException, ExecutionException {
            //do nothing
        }

        @Override
        public void run(Finished finished) {
            try {
                if (finished.isOk()) {
                    final RpcRequest.InstallSnapshotResponse response = RpcRequest.InstallSnapshotResponse.newBuilder()//
                            .setSuccess(true)//
                            .build();
                    installSnapshotCallback.setResponse(response);
                }
                ThreadUtil.runCallback(installSnapshotCallback, finished);
            } finally {
                try {
                    IOUtils.close(this.snapshotReader);
                } catch (IOException e) {
                }
            }
        }
    }

    public static class Option {


        private ReplicaOption replicaOption;
        private String storagePath;
        private SnapshotStorageFactory snapshotStorageFactory;
        private StateMachineCaller stateMachineCaller;
        private LogManager logManager;
        private PacificaClient pacificaClient;
        private int downloadSnapshotTimeoutMs;

        public PacificaClient getPacificaClient() {
            return pacificaClient;
        }

        public void setPacificaClient(PacificaClient pacificaClient) {
            this.pacificaClient = pacificaClient;
        }

        public ReplicaOption getReplicaOption() {
            return replicaOption;
        }

        public void setReplicaOption(ReplicaOption replicaOption) {
            this.replicaOption = replicaOption;
        }

        public String getStoragePath() {
            return storagePath;
        }

        public void setStoragePath(String storagePath) {
            this.storagePath = storagePath;
        }

        public SnapshotStorageFactory getSnapshotStorageFactory() {
            return snapshotStorageFactory;
        }

        public void setSnapshotStorageFactory(SnapshotStorageFactory snapshotStorageFactory) {
            this.snapshotStorageFactory = snapshotStorageFactory;
        }

        public StateMachineCaller getStateMachineCaller() {
            return stateMachineCaller;
        }

        public void setStateMachineCaller(StateMachineCaller stateMachineCaller) {
            this.stateMachineCaller = stateMachineCaller;
        }

        public LogManager getLogManager() {
            return logManager;
        }

        public void setLogManager(LogManager logManager) {
            this.logManager = logManager;
        }

        public int getDownloadSnapshotTimeoutMs() {
            return downloadSnapshotTimeoutMs;
        }

        public void setDownloadSnapshotTimeoutMs(int downloadSnapshotTimeoutMs) {
            this.downloadSnapshotTimeoutMs = downloadSnapshotTimeoutMs;
        }
    }


    static enum State {
        IDLE,
        SNAPSHOT_DOWNLOADING,
        SNAPSHOT_LOADING,
        SNAPSHOT_SAVING,
        UNINITIALIZED,
        SHUTTING,
        SHUTDOWN;
    }

}
