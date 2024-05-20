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
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.SnapshotDownloader;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.RpcLogUtil;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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


    public SnapshotManagerImpl(final ReplicaImpl replica) {
        this.replica = replica;
    }

    @Override
    public void init(Option option) throws PacificaException{
        this.writeLock.lock();
        try {
            if (this.state == State.UNINITIALIZED) {
                this.option = Objects.requireNonNull(option, "option");
                final String storagePath = Objects.requireNonNull(option.getStoragePath(), "storage path");
                final SnapshotStorageFactory snapshotStorageFactory = Objects.requireNonNull(option.getSnapshotStorageFactory(), "snapshot storage factory");
                this.snapshotStorage = snapshotStorageFactory.newSnapshotStorage(storagePath);
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
    public void startup() {
        this.writeLock.lock();
        try {
            if (this.state == State.SHUTDOWN) {
                try {
                    doFirstSnapshotLoad();
                } catch (Throwable throwable) {

                }
            }
        } finally {
            this.writeLock.unlock();
        }

    }

    @Override
    public void shutdown() {
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
    public void installSnapshot(RpcRequest.InstallSnapshotRequest installSnapshotRequest, Callback callback) {
        try {
            LogId installLogId = null;
            this.readLock.lock();
            try {
                final long snapshotLogIndex = installSnapshotRequest.getSnapshotLogIndex();
                final long snapshotLogTerm = installSnapshotRequest.getSnapshotLogTerm();
                installLogId = new LogId(snapshotLogIndex, snapshotLogTerm);
                if (installLogId.compareTo(this.lastSnapshotLogId) < 0) {
                    //TODO installed
                    LOGGER.warn("{} receive InstallSnapshotRequest({}), but has been installed. local_snapshot_log_id={}",
                            replica.getReplicaId(), RpcLogUtil.toLogString(installSnapshotRequest), this.lastSnapshotLogId);
                    throw new PacificaException(PacificaErrorCode.INTERNAL, "");
                }
            } finally {
                this.readLock.unlock();
            }
            if (STATE_UPDATER.compareAndSet(this, State.IDLE, State.SNAPSHOT_DOWNLOADING)) {
                final ReplicaId primaryId = RpcUtil.toReplicaId(installSnapshotRequest.getPrimaryId());
                //1、download snapshot from Primary
                final SnapshotStorage.DownloadContext context = new SnapshotStorage.DownloadContext(installLogId, installSnapshotRequest.getReaderId(), this.pacificaClient, primaryId);
                try(final SnapshotDownloader snapshotDownloader = this.snapshotStorage.startDownloadSnapshot(context)) {
                    this.snapshotDownloader = snapshotDownloader;
                    snapshotDownloader.start();
                    snapshotDownloader.awaitComplete();
                } catch (ExecutionException e) {
                    throw new PacificaException(PacificaErrorCode.USER_ERROR, "", e.getCause());
                } catch (InterruptedException e) {
                    //TODO
                } finally {
                    snapshotDownloader = null;
                }
                //2、load snapshot
                doSnapshotLoad(null, State.SNAPSHOT_DOWNLOADING);

            } else {
                //TODO
                return;
            }

        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        }
    }

    @Override
    public SnapshotStorage getSnapshotStorage() {
        return snapshotStorage;
    }

    private boolean isStarted() {
        return this.state.compareTo(State.UNINITIALIZED) > 0;
    }

    @Override
    public void doSnapshot(final Callback callback) {
        try {
            final int snapshotLogIndexMargin = Math.max(0, this.option.getReplicaOption().getSnapshotLogIndexMargin());
            if (STATE_UPDATER.compareAndSet(this, State.IDLE, State.SNAPSHOT_SAVING)) {
                this.readLock.lock();
                try {
                    final LogId commitPoint = this.stateMachineCaller.getCommitPoint();
                    if (commitPoint.getIndex() < this.lastSnapshotLogId.getIndex()) {
                        //TODO ERROR
                        return;
                    }
                    if (commitPoint.getIndex() == this.lastSnapshotLogId.getIndex()) {
                        //TODO
                        return;
                    }
                    final long distance = commitPoint.getIndex() - this.lastSnapshotLogId.getIndex();
                    if (distance < snapshotLogIndexMargin) {
                        //TODO
                        return;
                    }
                    final SnapshotSaveCallback snapshotSaveCallback = new SnapshotSaveCallback(callback);
                    this.stateMachineCaller.onSnapshotSave(snapshotSaveCallback);
                    LOGGER.info("");
                } finally {
                    this.readLock.unlock();
                }
            } else {
                throw new PacificaException("it is busy or not started. state=" + this.state);
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

                    } catch (ExecutionException e) {
                        throw new PacificaException(PacificaErrorCode.USER_ERROR, "", e.getCause());
                    }
                } else {
                    throw new PacificaException(PacificaErrorCode.BUSY, "");
                }
            } finally {
                STATE_UPDATER.set(this, State.IDLE);
            }
        } else {
            throw new PacificaException(PacificaErrorCode.BUSY, "");
        }
    }


    private void doFirstSnapshotLoad() throws PacificaException {
        final SnapshotReader snapshotReader = this.snapshotStorage.openSnapshotReader();
        final FirstSnapshotLoadCallback firstSnapshotLoadCallback = new FirstSnapshotLoadCallback(snapshotReader);
        doSnapshotLoad(firstSnapshotLoadCallback, State.IDLE);
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
        public SnapshotWriter start(LogId saveLogId) {
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
            }
        }

        public void awaitComplete() throws InterruptedException, ExecutionException {
            this.latch.await();
            if (this.error != null) {
                throw new ExecutionException(this.error);
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
