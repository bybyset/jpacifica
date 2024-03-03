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

import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.LogManager;
import com.trs.pacifica.LogStorage;
import com.trs.pacifica.LogStorageFactory;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LogManagerImpl implements LogManager, LifeCycle<LogManagerImpl.Option> {

    static final Logger LOGGER = LoggerFactory.getLogger(LogManagerImpl.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();

    private Option option;

    private SingleThreadExecutor executor;

    /***  start phase **/
    private LogStorage logStorage;

    /**
     * first log id at disk
     */
    private final LogId firstLogId = new LogId(0, 0);

    /**
     * last log id at disk
     */
    private final LogId lastLogId = new LogId(0, 0);

    private volatile long lastLogIndex = 0L;


    public LogManagerImpl() {
    }

    @Override
    public void init(LogManagerImpl.Option option) {
        this.writeLock.lock();
        try {
            this.option = Objects.requireNonNull(option);
            final ReplicaOption replicaOption = Objects.requireNonNull(option.getReplicaOption());
            this.executor = replicaOption.getExecutorGroup().chooseExecutor();
        } finally {
            this.writeLock.unlock();
        }


    }

    @Override
    public void startup() {
        this.writeLock.lock();
        try {
            final ReplicaOption replicaOption = Objects.requireNonNull(this.option.getReplicaOption());
            final LogStorageFactory logStorageFactory = Objects.requireNonNull(replicaOption.getPacificaServiceFactory());
            this.logStorage = logStorageFactory.newLogStorage(replicaOption.getLogStoragePath());
        } finally {
            this.writeLock.unlock();
        }

    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void appendLogEntries(List<LogEntry> logEntries, Callback callback) {

        this.writeLock.lock();
        try {
            // check resolve conflict
            // fill LogEntry -> LogId

            // fill LogEntry -> checksum

            

            //

        } finally {
            this.writeLock.unlock();
        }
    }



    @Override
    public LogEntry getLogEntryAt(long logIndex) {
        return null;
    }

    @Override
    public LogId getCommitPoint() {
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
    public void waitNewLog(long waitLogIndex, NewLogListener listener) {

    }


    private void doTruncatePrefix(final long firstLogIndexKept) {
        this.writeLock.lock();
        try {
            if (firstLogIndexKept < this.firstLogId.getIndex()) {
                return;
            }

            this.logStorage.truncatePrefix(firstLogIndexKept);

        } finally {
            this.writeLock.unlock();
        }

    }

    private void doTruncateSuffix(final long lastLogIndexKept) {

    }





    public static final class Option {

        ReplicaOption replicaOption;


        public ReplicaOption getReplicaOption() {
            return replicaOption;
        }

        public void setReplicaOption(ReplicaOption replicaOption) {
            this.replicaOption = replicaOption;
        }
    }
}
