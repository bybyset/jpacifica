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

package com.trs.pacifica.fsm;

import com.trs.pacifica.*;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private byte state = _STATE_UNINITIALIZED;

    /**
     * last committed log index
     */
    private volatile long lastCommitLogIndex = 0;

    /**
     * committing log index
     */
    private final AtomicLong committingLogIndex = new AtomicLong(0);


    private LogId commitPoint = new LogId(0, 0);

    private StateMachine stateMachine;

    private LogManager logManager;

    private SingleThreadExecutor eventExecutor;

    private PacificaException error = null;

    private PendingQueue<Callback> callbackPendingQueue = null;


    public StateMachineCallerImpl(ReplicaImpl replica) {
        this.replica = replica;
    }

    @Override
    public void init(Option option) {
        this.writeLock.lock();
        try {
            if (state == _STATE_UNINITIALIZED) {
                this.stateMachine = Objects.requireNonNull(option.getStateMachine(), "stateMachine");
                this.eventExecutor = Objects.requireNonNull(option.getExecutor(), "executor");
                this.callbackPendingQueue = Objects.requireNonNull(option.getCallbackPendingQueue(), "callbackPendingQueue");
                this.state = _STAT_SHUTDOWN;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void startup() {
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
    public void shutdown() {
        this.writeLock.lock();
        try {
            if (this.state == _STATE_STARTED) {
                this.state = _STAT_SHUTTING;

                this.state = _STAT_SHUTDOWN;
            }
        } finally {
            this.writeLock.unlock();
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
    public LogId getCommitPoint() {
        this.readLock.lock();
        try {
            return this.commitPoint.copy();
        } finally {
            this.readLock.unlock();
        }

    }

    @Override
    public long getCommittingLogIndex() {
        return this.committingLogIndex.get();
    }

    @Override
    public long getCommittedLogIndex() {
        return this.committingLogIndex.get() - 1;
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

    }

    private boolean submitEvent(final Runnable run) {
        assert run != null;
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

        final List<Callback> callbackList = new ArrayList<>();
        pollCallback(callbackList, commitLogIndex);
        final OperationIteratorImpl iterator = new OperationIteratorImpl(this.logManager, commitLogIndex, this.committingLogIndex, callbackList);

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
                    final OperationIteratorWrapper wrapper = new OperationIteratorWrapper(iterator);
                    this.stateMachine.onApply(wrapper);
                    logEntry = wrapper.getNextLogEntry();
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
        this.commitPoint = new LogId(commitLogIndex, commitLogTerm);
    }

    private void pollCallback(List<Callback> callbackList, final long commitPoint) {
        final List<Callback> callbacks = this.callbackPendingQueue.pollUntil(commitPoint);
        if (callbacks != null && !callbacks.isEmpty()) {
            callbackList.addAll(callbacks);
        }
    }

    private void setError(PacificaException error) {

    }

    private void doSnapshotLoad(final SnapshotLoadCallback snapshotLoadCallback) {
        final SnapshotReader snapshotReader = snapshotLoadCallback.getSnapshotReader();
        if (snapshotReader == null) {
            ThreadUtil.runCallback(snapshotLoadCallback, Finished.failure(new PacificaException("failed to get SnapshotReader")));
            return;
        }
        final LogId snapshotLogId = snapshotReader.getSnapshotLogId();
        if (snapshotLogId == null) {
            ThreadUtil.runCallback(snapshotLoadCallback, Finished.failure(new PacificaException("failed to get SnapshotMeta")));
            return;
        }
        // snapshotLogId > lastSnapshotLogId

        //keep snapshotLogId <= commitPoint

        //
        this.stateMachine.onSnapshotLoad(snapshotReader);


    }

    private void doSnapshotSave(final SnapshotSaveCallback snapshotSaveCallback) {
        assert snapshotSaveCallback != null;
        try {
            // meta
            final long snapshotLogIndex = this.commitPoint.getIndex();
            final long snapshotLogTerm = this.commitPoint.getTerm();
            final SnapshotWriter snapshotWriter = snapshotSaveCallback.start(new LogId(snapshotLogIndex, snapshotLogTerm));
            if (snapshotWriter == null) {
                throw new PacificaException("failed to get snapshot writer.");
            }
            this.stateMachine.onSnapshotSave(snapshotWriter);
            ThreadUtil.runCallback(snapshotSaveCallback, Finished.success());
        } catch (Throwable throwable) {
            ThreadUtil.runCallback(snapshotSaveCallback, Finished.failure(throwable));
        }
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


    public static class Option {

        private StateMachine stateMachine;
        private LogManager logManager;
        private SingleThreadExecutor executor;

        private PendingQueue<Callback> callbackPendingQueue;

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
    }
}
