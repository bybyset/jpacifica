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
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotMeta;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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


    public SnapshotManagerImpl(final ReplicaImpl replica) {
        this.replica = replica;
    }

    @Override
    public void init(Option option) {
        this.writeLock.lock();
        try {
            if (this.state == State.UNINITIALIZED) {
                this.option = Objects.requireNonNull(option, "option");
                final String storagePath = Objects.requireNonNull(option.getStoragePath(), "storage path");
                final SnapshotStorageFactory snapshotStorageFactory = Objects.requireNonNull(option.getSnapshotStorageFactory(), "snapshot storage factory");
                this.snapshotStorage = snapshotStorageFactory.newSnapshotStorage(storagePath);
                this.logManager = Objects.requireNonNull(option.getLogManager(), "logManager");
                this.stateMachineCaller = Objects.requireNonNull(option.getStateMachineCaller(), "stateMachineCaller");
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

                this.state = State.SHUTDOWN;
            }
        } finally {
            this.writeLock.unlock();
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
        final int snapshotLogIndexMargin = this.option.getReplicaOption().getSnapshotLogIndexMargin();
        if (snapshotLogIndexMargin < 0) {

            return;
        }
        if (STATE_UPDATER.compareAndSet(this, State.IDLE, State.SNAPSHOT_SAVING)) {
            this.writeLock.lock();
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

                final SnapshotWriter snapshotWriter = this.snapshotStorage.openSnapshotWriter();
                if (snapshotWriter == null) {
                    //TODO
                    return;
                }
                final SnapshotSaveCallback snapshotSaveCallback = new SnapshotSaveCallback(snapshotWriter, callback);
                this.stateMachineCaller.onSnapshotSave(snapshotSaveCallback);
                LOGGER.info("");

            } catch (Throwable throwable) {
                ThreadUtil.runCallback(callback, Finished.failure(throwable));
            } finally {
                this.writeLock.unlock();
                STATE_UPDATER.compareAndSet(this, State.SNAPSHOT_SAVING, State.IDLE);
            }
        } else {
            ThreadUtil.runCallback(callback, Finished.failure(new PacificaException("it is busy or not started. state=" + this.state)));
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

    @Override
    public String toString() {
        return "SnapshotManagerImpl{" +
                "state=" + state +
                ", lastSnapshotLogId=" + this.lastSnapshotLogId +
                '}';
    }

    private void doFirstSnapshotLoad() throws ExecutionException, InterruptedException {
        final SnapshotReader snapshotReader = this.snapshotStorage.openSnapshotReader();
        final FirstSnapshotLoadCallback firstSnapshotLoadCallback = new FirstSnapshotLoadCallback(snapshotReader);
        this.stateMachineCaller.onSnapshotLoad(firstSnapshotLoadCallback);
        firstSnapshotLoadCallback.awaitComplete();
    }

    private void onSnapshotLoadSuccess(final SnapshotMeta snapshotMeta) {
        this.writeLock.lock();
        try {
            final long snapshotLogIndex = snapshotMeta.getSnapshotLogIndex();
            final long snapshotLogTerm = snapshotMeta.getSnapshotLogTerm();
            final LogId snapshotLogId = new LogId(snapshotLogIndex, snapshotLogTerm);
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

    private void onSnapshotSaveSuccess() {

    }

    class SnapshotSaveCallback implements StateMachineCaller.SnapshotSaveCallback {

        private final SnapshotWriter snapshotWriter;

        private final Callback callback;

        SnapshotSaveCallback(SnapshotWriter snapshotWriter, Callback callback) {
            this.snapshotWriter = snapshotWriter;
            this.callback = callback;
        }

        @Override
        public SnapshotWriter getSnapshotWriter() {
            return snapshotWriter;
        }

        @Override
        public void run(Finished finished) {
            if (finished.isOk()) {
                onSnapshotSaveSuccess();
            }
            ThreadUtil.runCallback(callback, finished);

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
                    final SnapshotMeta meta = snapshotReader.getSnapshotMeta();
                    onSnapshotLoadSuccess(meta);
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
