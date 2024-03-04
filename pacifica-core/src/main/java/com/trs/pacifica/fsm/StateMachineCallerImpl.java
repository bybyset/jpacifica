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

import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.LogManager;
import com.trs.pacifica.StateMachine;
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
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

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();
    private byte state = _STATE_UNINITIALIZED;

    private final AtomicLong committingLogIndex = new AtomicLong(0);

    private volatile long lastCommittedLogIndex = 0;


    private StateMachine stateMachine;

    private LogManager logManager;

    private SingleThreadExecutor executor;


    @Override
    public void init(Option option) {
        this.writeLock.lock();
        try {
            if (state == _STATE_UNINITIALIZED) {
                this.stateMachine = Objects.requireNonNull(option.getStateMachine(), "stateMachine");
                this.executor = Objects.requireNonNull(option.getExecutor(), "executor");
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
        this.readLock.lock();
        try {
            if (logIndex <= this.lastCommittedLogIndex) {
                return false;
            }
            this.executor.execute(new CommitEvent(logIndex));
        } finally {
            this.readLock.unlock();
        }

        return true;
    }

    @Override
    public long getCommitPoint() {
        return 0;
    }

    @Override
    public boolean snapshotLoad() {
        return false;
    }

    @Override
    public boolean snapshotSave() {
        return false;
    }

    private void unsafeCommitAt(final long commitLogIndex) {
        if (commitLogIndex > this.lastCommittedLogIndex) {

            final OperationIteratorImpl iterator = new OperationIteratorImpl(this.logManager, commitLogIndex, this.committingLogIndex);
            while (iterator.hasNext()) {
                final LogEntry logEntry = iterator.next();
                if (logEntry == null) {
                    //TODO
                    break;
                }
                final LogEntry.Type type = logEntry.getType();
                switch (type) {
                    case OP_DATA:{
                        final OperationIteratorWrapper wrapper = new OperationIteratorWrapper(iterator);
                        this.stateMachine.onApply(wrapper);
                        break;
                    }
                    case NO_OP:
                    default:
                }
            }

            this.lastCommittedLogIndex = commitLogIndex;
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

        @Override
        public void run() {

        }
    }

    class SnapshotSaveEvent implements Runnable {

        @Override
        public void run() {

        }
    }


    public static class Option {

        private StateMachine stateMachine;

        private LogManager logManager;
        private SingleThreadExecutor executor;

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
    }
}
