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

import com.trs.pacifica.BallotBox;
import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.StateMachineCaller;
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

    private long pendingLogIndex = 0, lastCommittedLogIndex = 0;

    private StateMachineCaller fsmCaller;

    private final LinkedList<Ballot> ballotQueue = new LinkedList<>();

    @Override
    public void init(Option option) {
        this.writeLock.lock();
        try {
            this.fsmCaller = Objects.requireNonNull(option.getFsmCaller(), "option.getFsmCaller()");

        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void startup() {
        this.writeLock.lock();
        try {
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
        return false;
    }

    @Override
    public boolean recoverBallot(ReplicaId replicaId, long startLogIndex) {
        return false;
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
            if (this.ballotQueue.isEmpty()) {
                return false;
            }
            if (this.pendingLogIndex == 0) {
                return false;
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
        return true;
    }


    public static class Option {

        private StateMachineCaller fsmCaller;

        public StateMachineCaller getFsmCaller() {
            return fsmCaller;
        }

        public void setFsmCaller(StateMachineCaller fsmCaller) {
            this.fsmCaller = fsmCaller;
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
