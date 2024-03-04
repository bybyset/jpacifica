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
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LogManagerImpl implements LogManager, LifeCycle<LogManagerImpl.Option> {

    static final Logger LOGGER = LoggerFactory.getLogger(LogManagerImpl.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();

    private final Map<Long, NewLogContext> newLogWaiterContainer = new ConcurrentHashMap<>();

    private final AtomicLong waiterIdAllocator = new AtomicLong();
    private Option option;

    private SingleThreadExecutor executor;

    /***  start phase **/
    private LogStorage logStorage;

    /**
     * first log id at disk
     */
    private final LogId firstLogId = new LogId(0, 0);

    private LogId committedPoint = new LogId(0, 0);
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

    private long unsafeGetLogTermAt(final long index) {

        return 0L;
    }

    private boolean checkAndResolveConflict(final List<LogEntry> logEntries, Callback callback) {

        assert logEntries.isEmpty() == false;
        final LogEntry firstLogEntry = logEntries.get(0);
        boolean primary = firstLogEntry.getLogId().getIndex() == 0;
        if (primary) {
            // fill LogEntry -> LogId.index
            logEntries.forEach(logEntry -> {
                logEntry.getLogId().setIndex(++this.lastLogIndex);
            });
            return true;
        } else {
            //first
            if (firstLogEntry.getLogId().getIndex() > this.lastLogIndex + 1) {
                //discontinuous
                ThreadUtil.runCallback(callback, null);
                return false;
            }
            final LogEntry lastLogEntry = logEntries.get(logEntries.size() - 1);
            if (lastLogEntry.getLogId().getIndex() <= this.committedPoint.getIndex()) {
                //has been committed
                ThreadUtil.runCallback(callback, null);
                return false;
            }

            if (firstLogEntry.getLogId().getIndex() != this.lastLogIndex + 1) {
                // resolve conflict
                // 1、find conflict LogEntry
                int conflictingIndex = 0;
                for (; conflictingIndex < logEntries.size(); conflictingIndex++) {
                    final LogEntry curLogEntry = logEntries.get(conflictingIndex);
                    if (unsafeGetLogTermAt(curLogEntry.getLogId().getIndex()) != curLogEntry.getLogId().getTerm()) {
                        break;
                    }
                }
                if (conflictingIndex != logEntries.size()) {
                    final LogEntry conflictingLogEntry = logEntries.get(conflictingIndex);
                    if (conflictingLogEntry.getLogId().getIndex() <= this.lastLogIndex) {
                        //has conflict log: truncate suffix
                        truncateSuffix(conflictingLogEntry.getLogId().getIndex() - 1, null);
                    }
                }
                if (conflictingIndex > 0) {
                    //remove duplication
                    logEntries.subList(0, conflictingIndex).clear();
                }
            }
            this.lastLogIndex = lastLogEntry.getLogId().getIndex();
            return true;

        }
    }

    @Override
    public void appendLogEntries(List<LogEntry> logEntries, AppendLogEntriesCallback callback) {

        this.writeLock.lock();
        try {
            if (logEntries.isEmpty()) {

            }


            // check resolve conflict
            checkAndResolveConflict(logEntries, callback);

            if (logEntries.isEmpty()) {
                //TODO

            }
            final LogEntry firstLogEntry = logEntries.get(0);
            // fill LogEntry -> checksum
            final boolean enableLogEntryChecksum = this.option.getReplicaOption().isEnableLogEntryChecksum();
            if (enableLogEntryChecksum) {
                logEntries.forEach(logEntry -> {
                    if (logEntry.hasChecksum() == false) {
                        logEntry.setChecksum(logEntry.checksum());
                    }
                });
            }

            // store log entries
            storeLogEntries(logEntries, callback);
        } finally {
            this.writeLock.unlock();
        }
    }


    @Override
    public LogEntry getLogEntryAt(long logIndex) {
        return null;
    }

    @Override
    public long getLogTermAt(long logIndex) {
        return 0;
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
    public long waitNewLog(final long expectedLastLogIndex, final NewLogWaiter newLogWaiter) {
        Objects.requireNonNull(newLogWaiter, "newLogWaiter");
        this.readLock.lock();
        try {
            final long lastLogIndexOnDisk = getLastLogIndexOnDisk();
            if (expectedLastLogIndex <= lastLogIndexOnDisk) {
                newLogWaiter.setNewLogIndex(lastLogIndexOnDisk);
                ThreadUtil.runCallback(newLogWaiter, null);
                return -1L;
            } else {
                final NewLogContext onNewLog = new NewLogContext(expectedLastLogIndex, newLogWaiter);
                final long waiterId = this.waiterIdAllocator.incrementAndGet();
                this.newLogWaiterContainer.putIfAbsent(waiterId, onNewLog);
                return waiterId;
            }
        } finally {
            this.readLock.unlock();
        }

    }

    @Override
    public boolean removeWaiter(long waiterId) {
        return this.newLogWaiterContainer.remove(waiterId) != null;
    }

    long getLastLogIndexOnDisk() {
        return 0L;
    }

    private void notifyNewLog() {
        this.readLock.lock();
        final long lastLogIndexOnDisk = getLastLogIndexOnDisk();
        try {
            List<Long> notifyWaiterIds = new ArrayList<>();
            //find
            this.newLogWaiterContainer.forEach((waiterId, newLogWaiter) -> {
                if (newLogWaiter.expectedLastLogIndex <= lastLogIndexOnDisk) {
                    notifyWaiterIds.add(waiterId);
                }
            });
            //run
            notifyWaiterIds.forEach(waiterId -> {
                NewLogContext newLogContext = newLogWaiterContainer.remove(waiterId);
                if (newLogContext != null) {
                    ThreadUtil.runCallback(newLogContext.newLogCallback, Finished._OK);
                }
            });
        } finally {
            this.readLock.unlock();
        }
    }

    void storeLogEntries(final List<LogEntry> logEntries, final AppendLogEntriesCallback callback) {
        this.executor.execute(new StoreLogEntriesEvent(logEntries, callback));
    }

    private void doStoreLogEntries(final List<LogEntry> logEntries) {

        final int storedNum = this.logStorage.appendLogEntries(logEntries);
        if (storedNum > 0) {

        }

    }

    void truncateSuffix(final long lastIndexKept, final Callback callback) {
        this.executor.execute(new TruncateSuffixEvent(lastIndexKept));
    }

    private void doTruncateSuffix(final long lastIndexKept) {

        this.logStorage.truncateSuffix(lastIndexKept);

    }

    void truncatePrefix(final long firstIndexKept, final Callback callback) {
        this.executor.execute(new TruncatePrefixEvent(firstIndexKept));
    }

    class StoreLogEntriesEvent implements Runnable {

        private final List<LogEntry> logEntries;

        private final AppendLogEntriesCallback callback;

        StoreLogEntriesEvent(List<LogEntry> logEntries, AppendLogEntriesCallback callback) {
            this.logEntries = logEntries;
            this.callback = callback;
        }

        @Override
        public void run() {

        }
    }

    class TruncateSuffixEvent implements Runnable {

        private final long lastIndexKept;

        TruncateSuffixEvent(final long lastIndexKept) {
            this.lastIndexKept = lastIndexKept;
        }

        @Override
        public void run() {

        }
    }

    class TruncatePrefixEvent implements Runnable {


        private final long firstIndexKept;

        TruncatePrefixEvent(final long firstIndexKept) {
            this.firstIndexKept = firstIndexKept;
        }


        @Override
        public void run() {

        }
    }

    private static class NewLogContext implements Comparable<NewLogContext> {

        private final long expectedLastLogIndex;

        private final NewLogWaiter newLogCallback;

        NewLogContext(final long expectedLastLogIndex, NewLogWaiter newLogCallback) {
            this.expectedLastLogIndex = expectedLastLogIndex;
            this.newLogCallback = newLogCallback;
        }

        @Override
        public int compareTo(NewLogContext o) {
            if (o == null) {
                return 1;
            }
            return Long.compare(expectedLastLogIndex, o.expectedLastLogIndex);
        }
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
