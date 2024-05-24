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
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BallotBoxImpl implements BallotBox, LifeCycle<BallotBoxImpl.Option> {
    static final Logger LOGGER = LoggerFactory.getLogger(BallotBoxImpl.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private long pendingLogIndex = 0; //pendingLogIndex = last_log_index + 1
    private volatile long lastCommittedLogIndex = 0; //lastCommittedLogIndex
    private StateMachineCaller fsmCaller;
    private LogManager logManager;
    private final LinkedList<Ballot> ballotQueue = new LinkedList<>();
    private final PendingQueue<Ballot> ballotPendingQueue = new PendingQueueImpl<>();

    @Override
    public void init(Option option) {
        this.writeLock.lock();
        try {
            this.fsmCaller = Objects.requireNonNull(option.getFsmCaller(), "option.getFsmCaller()");
            this.logManager = Objects.requireNonNull(option.getLogManager(), "option.getLogManager()");
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void startup() {
        this.writeLock.lock();
        try {
            this.pendingLogIndex = this.logManager.getLastLogId().getIndex();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            reset();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean initiateBallot(ReplicaGroup replicaGroup) {
        final ReplicaId primary = replicaGroup.getPrimary();
        final List<ReplicaId> secondaryList = replicaGroup.listSecondary();
        final Ballot ballot = new Ballot(primary);
        secondaryList.forEach(replicaId -> ballot.addGranter(replicaId));
        this.writeLock.lock();
        try {
            this.ballotQueue.add(ballot);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean cancelBallot(ReplicaId replicaId) {
        Objects.requireNonNull(replicaId, "replicaId");
        this.readLock.lock();
        long lastCommittedLogIndex = this.lastCommittedLogIndex;
        try {
            if (this.pendingLogIndex == 0) {
                return false;
            }
            if (this.ballotQueue.isEmpty()) {
                return true;
            }
            int index = 0;
            for (Ballot ballot : this.ballotQueue) {
                if (ballot.removeGranter(replicaId) && ballot.isGranted()) {
                    lastCommittedLogIndex += index;
                }
                index++;
            }
            if (lastCommittedLogIndex == this.lastCommittedLogIndex) {
                return true;
            }

        } finally {
            this.readLock.unlock();
        }
        setLastCommittedLogIndex(lastCommittedLogIndex);
        return true;
    }

    @Override
    public boolean recoverBallot(final ReplicaId replicaId, long startLogIndex) {
        Objects.requireNonNull(replicaId, "replicaId");
        this.readLock.lock();
        try {
            if (startLogIndex < this.pendingLogIndex) {
                return false;
            }
            if (startLogIndex < this.lastCommittedLogIndex) {
                // has committed
                return false;
            }
            final int fromIndex = (int) Math.max(0, startLogIndex - pendingLogIndex);
            final int toIndex = this.ballotQueue.size();
            List<Ballot> recoverList = this.ballotQueue.subList(fromIndex, toIndex);
            recoverList.forEach(ballot -> ballot.addGranter(replicaId));
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    @Override
    public boolean ballotBy(final ReplicaId replicaId, long startLogIndex, long endLogIndex) {
        assert startLogIndex <= endLogIndex;
        if (startLogIndex > endLogIndex) {
            throw new IllegalArgumentException(String.format("startLogIndex(%d) greater than endLogIndex(%d).", startLogIndex, endLogIndex));
        }
        this.readLock.lock();
        long lastCommittedLogIndex = this.lastCommittedLogIndex;
        try {
            if (this.pendingLogIndex == 0) {
                return false;
            }
            if (this.ballotQueue.isEmpty()) {
                return true;
            }
            if (endLogIndex < pendingLogIndex) {
                return true;
            }
            startLogIndex = Math.max(this.pendingLogIndex, startLogIndex);
            endLogIndex = Math.min(endLogIndex, this.pendingLogIndex + this.ballotQueue.size());
            final int fromIndex = (int) Math.max(0, startLogIndex - pendingLogIndex);
            final int toIndex = (int) Math.max(1, endLogIndex - pendingLogIndex + 1);
            List<Ballot> commitList = this.ballotQueue.subList(fromIndex, toIndex);
            int index = 0;
            for (Ballot ballot : commitList) {
                if (ballot.grant(replicaId)) {
                    lastCommittedLogIndex += index;
                }
                index++;
            }
            if (lastCommittedLogIndex == this.lastCommittedLogIndex) {
                return true;
            }
        } finally {
            this.readLock.unlock();
        }
        setLastCommittedLogIndex(lastCommittedLogIndex);
        return true;
    }

    @Override
    public Lock getCommitLock() {
        return this.writeLock;
    }

    @Override
    public long getLastCommittedLogIndex() {
        return this.lastCommittedLogIndex;
    }

    private void setLastCommittedLogIndex(final long lastCommittedLogIndex) {
        this.writeLock.lock();
        try {
            if (lastCommittedLogIndex >= this.lastCommittedLogIndex) {
                this.lastCommittedLogIndex = lastCommittedLogIndex;
                this.fsmCaller.commitAt(lastCommittedLogIndex);
            }
            while (this.pendingLogIndex <= lastCommittedLogIndex) {
                this.ballotQueue.poll();
                this.pendingLogIndex++;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    void reset() {
        this.pendingLogIndex = 0;
        this.lastCommittedLogIndex = 0;
        this.ballotQueue.clear();
    }


    public static class Option {

        private StateMachineCaller fsmCaller;

        private LogManager logManager;

        public StateMachineCaller getFsmCaller() {
            return fsmCaller;
        }
        public void setFsmCaller(StateMachineCaller fsmCaller) {
            this.fsmCaller = fsmCaller;
        }

        public LogManager getLogManager() {
            return logManager;
        }

        public void setLogManager(LogManager logManager) {
            this.logManager = logManager;
        }
    }

    static class Ballot {

        static final int DEFAULT_GRANTER_NUM = 3;
        static final AtomicIntegerFieldUpdater<Ballot> ATOMIC_QUORUM = AtomicIntegerFieldUpdater.newUpdater(Ballot.class, "quorum");
        private volatile int quorum = 0;

        private Map<ReplicaId, Granter> granters = Collections.synchronizedMap(new HashMap<>(DEFAULT_GRANTER_NUM));

        Ballot() {

        }

        Ballot(ReplicaId... replicaIds) {
            this(Arrays.asList(replicaIds));
        }

        Ballot(Collection<ReplicaId> replicaIds) {
            for (ReplicaId replicaId : replicaIds) {
                addGranter(replicaId);
            }
        }

        public boolean isGranted() {
            return quorum == 0;
        }

        /**
         * @param replicaId
         * @return {@link #isGranted()}
         */
        public boolean grant(final ReplicaId replicaId) {
            final Granter granter = this.granters.get(replicaId);
            if (granter != null && granter.grant()) {
                ATOMIC_QUORUM.decrementAndGet(this);
            }
            return isGranted();
        }

        public boolean addGranter(final ReplicaId replicaId) {
            if (granters.containsKey(replicaId)) {
                return true;
            }
            final Granter granter = granters.putIfAbsent(replicaId, new Granter(replicaId));
            if (granter == null) {
                ATOMIC_QUORUM.incrementAndGet(this);
            }
            return true;
        }

        public boolean removeGranter(final ReplicaId replicaId) {
            final Granter granter = granters.remove(replicaId);
            if (granter == null) {
                return false;
            }
            if (granter.grant()) {
                ATOMIC_QUORUM.decrementAndGet(this);
            }
            return true;
        }

    }


    static class Granter {

        private final ReplicaId replicaId;

        private AtomicBoolean granted = new AtomicBoolean(false);

        Granter(ReplicaId replicaId) {
            this.replicaId = replicaId;
        }

        boolean hasGranted() {
            return granted.get();
        }

        boolean grant() {
            return this.granted.compareAndSet(false, true);
        }

    }

}
