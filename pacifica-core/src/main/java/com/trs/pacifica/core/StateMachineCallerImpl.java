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
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.core.fsm.OperationIteratorImpl;
import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.core.fsm.OperationIteratorWrapper;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.OnlyForTest;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * CommittedPoint <= LastAppliedLogIndex <= LastCommittedLogIndex
 */
public class StateMachineCallerImpl implements StateMachineCaller, LifeCycle<StateMachineCallerImpl.Option> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachineCallerImpl.class);

    private static final byte _STATE_UNINITIALIZED = 0;
    private static final byte _STATE_STARTED = 1;
    private static final byte _STAT_SHUTTING = 2;
    private static final byte _STAT_SHUTDOWN = 3;

    private final ReplicaImpl replica;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private volatile byte state = _STATE_UNINITIALIZED;

    /**
     * last committed log index, it's increasing.
     * Reject commit events smaller than it
     */
    private volatile long lastCommitLogIndex = 0;

    /**
     * applying log index
     */
    private final AtomicLong applyingLogIndex = new AtomicLong(1);

    /**
     * it is possible: committedPont <= lastAppliedLogIndex
     * It is useful for do snapshot
     */
    private LogId committedPont = new LogId(0, 0);


    private StateMachine stateMachine;
    private LogManager logManager;
    private SingleThreadExecutor eventExecutor;
    private volatile PacificaException error = null;
    private PendingQueue<Callback> callbackPendingQueue = null;

    public StateMachineCallerImpl(ReplicaImpl replica) {
        this.replica = replica;
    }

    @Override
    public void init(Option option) throws PacificaException {
        this.writeLock.lock();
        try {
            if (state == _STATE_UNINITIALIZED) {
                this.stateMachine = Objects.requireNonNull(option.getStateMachine(), "stateMachine");
                this.eventExecutor = Objects.requireNonNull(option.getExecutor(), "executor");
                this.callbackPendingQueue = Objects.requireNonNull(option.getCallbackPendingQueue(), "callbackPendingQueue");
                this.logManager = Objects.requireNonNull(option.getLogManager(), "logManager");
                final LogId bootstrapId = option.getBootstrapId() == null ? new LogId(0, 0) : option.getBootstrapId();
                this.committedPont = bootstrapId.copy();
                this.applyingLogIndex.set(bootstrapId.getIndex() + 1);
                this.lastCommitLogIndex = bootstrapId.getIndex();
                this.state = _STAT_SHUTDOWN;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void startup() throws PacificaException {
        this.writeLock.lock();
        try {
            if (this.state == _STAT_SHUTDOWN) {
                this.state = _STATE_STARTED;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() throws PacificaException {
        try {
            ShutdownEvent shutdownEvent = new ShutdownEvent();
            while (true) {
                if (submitEvent(shutdownEvent)) {
                    shutdownEvent.await();
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("{}-{} is interrupted on shutdown", this.replica.getReplicaId(), this.getClass().getSimpleName());
        }
    }

    public boolean isStarted() {
        return this.state == _STATE_STARTED;
    }

    private void ensureStarted() {
        if (!isStarted()) {
            throw new AlreadyClosedException(String.format("%s(%s) is stopped.", this.getClass().getSimpleName(), this.replica.getReplicaId()));
        }
    }

    private void doShutdown() {
        if (this.state == _STATE_STARTED) {
            this.writeLock.lock();
            try {
                this.state = _STAT_SHUTTING;
                //
                this.stateMachine.onShutdown();
                LOGGER.info("{} StateMachine of user is shutdown.", this.replica.getReplicaId());
                this.state = _STAT_SHUTDOWN;
            } finally {
                this.writeLock.unlock();
            }
        }
    }

    @Override
    public boolean commitAt(final long logIndex) {
        if (logIndex <= this.lastCommitLogIndex) {
            return false;
        }
        this.readLock.lock();
        try {
            this.eventExecutor.execute(new CommitEvent(logIndex));
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    @Override
    public long getLastAppliedLogIndex() {
        return this.applyingLogIndex.get() - 1;
    }

    @Override
    public long getLastCommittedLogIndex() {
        return this.lastCommitLogIndex;
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotLoadCallback snapshotLoadCallback) {
        return submitEvent(new SnapshotLoadEvent(snapshotLoadCallback));
    }

    @Override
    public boolean onSnapshotSave(final SnapshotSaveCallback snapshotSaveCallback) {
        return submitEvent(new SnapshotSaveEvent(snapshotSaveCallback));
    }

    @Override
    public void onError(PacificaException error) {
        if (error != null) {
            submitEvent(new FaultEvent(error));
        }
    }

    LogId getCommittedPont() {
        this.readLock.lock();
        try {
            return this.committedPont.copy();
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean submitEvent(final Runnable run) {
        assert run != null;
        ensureStarted();
        try {
            this.eventExecutor.execute(run);
        } catch (RejectedExecutionException e) {
            LOGGER.warn("{} reject to submit event.", this.replica.getReplicaId(), e);
            return false;
        }
        return true;
    }

    private void unsafeCommitAt(final long commitLogIndex) {
        if (commitLogIndex <= this.lastCommitLogIndex) {
            return;
        }
        this.lastCommitLogIndex = commitLogIndex;
        final OperationIteratorImpl iterator = new OperationIteratorImpl(this.logManager, commitLogIndex, this.applyingLogIndex, this.callbackPendingQueue);
        LogEntry logEntry = iterator.hasNext() ? iterator.next() : null;
        while (logEntry != null) {
            if (iterator.hasError()) {
                //
                setError(iterator.getError());
                break;
            }
            final LogEntry.Type type = logEntry.getType();
            switch (type) {
                case OP_DATA: {
                    final OperationIteratorWrapper wrapper = new OperationIteratorWrapper(iterator, this);
                    this.stateMachine.onApply(wrapper);
                    logEntry = wrapper.hasError()? null : wrapper.getNextLogEntry();
                    break;
                }
                case NO_OP:
                default: {
                    logEntry = iterator.hasNext() ? iterator.next() : null;// next
                    break;
                }
            }

        }
        if (iterator.hasError()) {
            setError(iterator.getError());
        }
        final long commitLogTerm = this.logManager.getLogTermAt(commitLogIndex);
        this.committedPont = new LogId(commitLogIndex, commitLogTerm);
    }

    private void setError(PacificaException fault) {
        this.writeLock.lock();
        try {
            if (this.error != null) {
                LOGGER.error("{} a failure has occurred. Repeat the report.", this.replica.getReplicaId(), fault);
                return;
            }
            this.error = fault;
            if (this.stateMachine != null) {
                this.stateMachine.onError(fault);
            }
            if (this.replica != null) {
                this.replica.onError(fault);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @OnlyForTest
    PacificaException getError() {
        return this.error;
    }

    /**
     * thread safe
     *
     * @param snapshotLoadCallback
     */
    private void doSnapshotLoad(final SnapshotLoadCallback snapshotLoadCallback) {
        this.writeLock.lock();
        try {
            final SnapshotReader snapshotReader = snapshotLoadCallback.getSnapshotReader();
            if (snapshotReader == null) {
                throw new PacificaException(PacificaErrorCode.INTERNAL, "failed to get SnapshotReader");
            }
            final LogId snapshotLogId = snapshotReader.getSnapshotLogId();
            if (snapshotLogId == null) {
                throw new PacificaException(PacificaErrorCode.INTERNAL, "failed to get SnapshotMeta");
            }
            // commitPoint > snapshotLogId ,
            if (this.committedPont.compareTo(snapshotLogId) > 0) {
                throw new PacificaException(PacificaErrorCode.INTERNAL, String.format("load snapshot, but snapshot_log_id=%s less than commit_point=%s.", snapshotLogId, committedPont));
            }
            // do load state machine of user
            this.stateMachine.onSnapshotLoad(snapshotReader);
            // reset need in write lock block
            this.committedPont = snapshotLogId.copy();
            this.lastCommitLogIndex = snapshotLogId.getIndex();
            this.applyingLogIndex.set(snapshotLogId.getIndex() + 1);
            ThreadUtil.runCallback(snapshotLoadCallback, Finished.success());
        } catch (Throwable e) {
            ThreadUtil.runCallback(snapshotLoadCallback, Finished.failure(e));
        } finally {
            this.writeLock.unlock();
        }
    }

    private void doSnapshotSave(final SnapshotSaveCallback snapshotSaveCallback) {
        assert snapshotSaveCallback != null;
        this.readLock.lock();
        try {
            // meta
            final long snapshotLogIndex = this.committedPont.getIndex();
            final long snapshotLogTerm = this.committedPont.getTerm();
            final SnapshotWriter snapshotWriter = snapshotSaveCallback.start(new LogId(snapshotLogIndex, snapshotLogTerm));
            if (snapshotWriter == null) {
                throw new PacificaException(PacificaErrorCode.INTERNAL, "failed to get snapshot writer.");
            }
            this.stateMachine.onSnapshotSave(snapshotWriter);
            ThreadUtil.runCallback(snapshotSaveCallback, Finished.success());
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(snapshotSaveCallback, Finished.failure(throwable));
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * flush all events that are currently submitted.
     *
     * @throws InterruptedException
     */
    @OnlyForTest
    void flush() throws InterruptedException {
        FlushEvent flushEvent = new FlushEvent();
        submitEvent(flushEvent);
        flushEvent.await();
    }

    class CommitEvent implements Runnable {

        long commitLogIndex;

        CommitEvent(final long commitLogIndex) {
            this.commitLogIndex = commitLogIndex;
        }


        @Override
        public void run() {
            unsafeCommitAt(commitLogIndex);
        }

    }

    class SnapshotLoadEvent implements Runnable {

        private final SnapshotLoadCallback snapshotLoadCallback;

        SnapshotLoadEvent(SnapshotLoadCallback snapshotLoadCallback) {
            this.snapshotLoadCallback = snapshotLoadCallback;
        }

        @Override
        public void run() {
            doSnapshotLoad(snapshotLoadCallback);
        }
    }

    class SnapshotSaveEvent implements Runnable {

        private final SnapshotSaveCallback snapshotSaveCallback;

        SnapshotSaveEvent(SnapshotSaveCallback snapshotSaveCallback) {
            this.snapshotSaveCallback = snapshotSaveCallback;
        }

        @Override
        public void run() {
            doSnapshotSave(snapshotSaveCallback);
        }
    }

    class FaultEvent implements Runnable {

        private final PacificaException fault;

        FaultEvent(PacificaException fault) {
            this.fault = fault;
        }

        @Override
        public void run() {
            setError(fault);
        }
    }

    class FlushEvent implements Runnable {

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        @Override
        public void run() {
            countDownLatch.countDown();
        }

        void await() throws InterruptedException {
            this.countDownLatch.await();
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return this.countDownLatch.await(timeout, unit);
        }


    }

    class ShutdownEvent implements Runnable {

        private CountDownLatch countDownLatch = new CountDownLatch(1);
        @Override
        public void run() {
            try {
                doShutdown();
            } finally {
                this.countDownLatch.countDown();
            }
        }

        void await() throws InterruptedException {
            this.countDownLatch.await();
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return this.countDownLatch.await(timeout, unit);
        }
    }



    public static class Option {

        private StateMachine stateMachine;
        private LogManager logManager;
        private SingleThreadExecutor executor;
        private PendingQueue<Callback> callbackPendingQueue;
        private LogId bootstrapId = new LogId(0, 0);

        public StateMachine getStateMachine() {
            return stateMachine;
        }

        public void setStateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
        }

        public LogManager getLogManager() {
            return logManager;
        }

        public void setLogManager(LogManager logManager) {
            this.logManager = logManager;
        }

        public SingleThreadExecutor getExecutor() {
            return executor;
        }

        public void setExecutor(SingleThreadExecutor executor) {
            this.executor = executor;
        }

        public PendingQueue<Callback> getCallbackPendingQueue() {
            return callbackPendingQueue;
        }

        public void setCallbackPendingQueue(PendingQueue<Callback> callbackPendingQueue) {
            this.callbackPendingQueue = callbackPendingQueue;
        }

        public LogId getBootstrapId() {
            return bootstrapId;
        }

        public void setBootstrapId(LogId bootstrapId) {
            this.bootstrapId = bootstrapId;
        }
    }
}
