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
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.sender.Sender;
import com.trs.pacifica.sender.SenderGroup;
import com.trs.pacifica.sender.SenderType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SenderGroupImpl implements SenderGroup, LifeCycle<SenderGroupImpl.Option> {

    static final Logger LOGGER = LoggerFactory.getLogger(SenderGroupImpl.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();

    private final PacificaClient pacificaClient;

    private final Map<ReplicaId, Sender> senderContainer = new ConcurrentHashMap<>();

    private Option option;

    public SenderGroupImpl(PacificaClient pacificaClient) {
        this.pacificaClient = pacificaClient;
    }

    @Override
    public boolean addSenderTo(final ReplicaId replicaId, SenderType senderType, boolean checkConnection) {
        Objects.requireNonNull(replicaId, "replicaId");
        this.readLock.lock();
        try {
            Sender sender = this.senderContainer.get(replicaId);
        } finally {
            this.readLock.unlock();
        }



        return false;
    }

    @Override
    public boolean isAlive(final ReplicaId replicaId) {
        Objects.requireNonNull(replicaId, "replicaId");
        this.readLock.lock();
        try {
            final Sender sender = this.senderContainer.get(replicaId);
            if (sender == null) {
                return false;
            }
            return sender.isAlive(this.option.getLeasePeriodTimeOutMs());
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public boolean continueAppendLogEntry(long logIndex) {
        return false;
    }

    @Override
    public Sender removeSender(ReplicaId replicaId) {
        return null;
    }

    @Override
    public void clear() {

    }


    @Override
    public synchronized void init(SenderGroupImpl.Option option) {
        this.option = Objects.requireNonNull(option, "option");
    }

    @Override
    public synchronized void startup() {

    }

    @Override
    public synchronized void shutdown() {

    }


    public static class Option {
        private LogManager logManager;

        private StateMachineCaller stateMachineCaller;

        private int leasePeriodTimeOutMs;

        public int getLeasePeriodTimeOutMs() {
            return leasePeriodTimeOutMs;
        }

        public void setLeasePeriodTimeOutMs(int leasePeriodTimeOutMs) {
            this.leasePeriodTimeOutMs = leasePeriodTimeOutMs;
        }

        public LogManager getLogManager() {
            return logManager;
        }

        public void setLogManager(LogManager logManager) {
            this.logManager = logManager;
        }

        public StateMachineCaller getStateMachineCaller() {
            return stateMachineCaller;
        }

        public void setStateMachineCaller(StateMachineCaller stateMachineCaller) {
            this.stateMachineCaller = stateMachineCaller;
        }
    }
}
