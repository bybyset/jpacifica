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

package com.trs.pacifica.sender;

import com.trs.pacifica.*;
import com.trs.pacifica.async.thread.ExecutorGroup;
import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.fs.FileService;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SenderGroupImpl implements SenderGroup, LifeCycle<SenderGroupImpl.Option> {

    static final Logger LOGGER = LoggerFactory.getLogger(SenderGroupImpl.class);

    static final byte STATE_UNINITIALIZED = 0x00;
    static final byte STATE_STARTED = 0x01;
    static final byte STATE_SHUTDOWN = 0x02;


    private final ReplicaImpl replica;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();

    private final PacificaClient pacificaClient;

    private final Map<ReplicaId, Sender> senderContainer = new ConcurrentHashMap<>();

    private final SenderImplFactory senderFactory = new SenderImplFactory();

    private volatile byte state = STATE_UNINITIALIZED;


    private Option option;

    private ExecutorGroup senderExecutorGroup;

    private LogManager logManager;

    private StateMachineCaller stateMachineCaller;

    private BallotBox ballotBox;

    private ReplicaGroup replicaGroup;

    private ConfigurationClient configurationClient;

    private FileService fileService;

    public SenderGroupImpl(ReplicaImpl replica, PacificaClient pacificaClient) {
        this.replica = replica;
        this.pacificaClient = Objects.requireNonNull(pacificaClient, "pacificaClient");
    }

    @Override
    public void addSenderTo(final ReplicaId replicaId, final SenderType senderType, boolean checkConnection) {
        Objects.requireNonNull(replicaId, "replicaId");
        this.readLock.lock();
        try {
            ensureStarted();
            if (checkConnection) {
                if (!this.pacificaClient.checkConnection(replicaId, true)) {
                    throw new PacificaException(String.format("%s fails to connect to %s", this.replica.getReplicaId(), replicaId));
                }
            }
            //TODO if has add??
            Sender sender = this.senderContainer.computeIfAbsent(replicaId, id -> {
                return senderFactory.getSender(id, senderType);
            });
            sender.startup();
            LOGGER.info("{} success to add sender to {}.", this.replica.getReplicaId(), replicaId);
        } finally {
            this.readLock.unlock();
        }
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
    public boolean waitCaughtUp(ReplicaId replicaId, Sender.OnCaughtUp onCaughtUp, long timeoutMs) {
        this.readLock.lock();
        try {
            Sender sender = this.senderContainer.get(replicaId);
            if (sender != null) {
                sender.waitCaughtUp(onCaughtUp, timeoutMs);
            }
        } finally {
            this.readLock.unlock();
        }

        return false;
    }

    @Override
    public boolean continueAppendLogEntry(long logIndex) {
        this.readLock.lock();
        try {
            ensureStarted();
            senderContainer.forEach((id, sender) -> {
                sender.continueSendLogEntries(logIndex);
            });
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    @Override
    public Sender removeSender(final ReplicaId replicaId) {
        this.readLock.lock();
        try {
            removeAndShutdown(replicaId);
            return null;
        }finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void init(SenderGroupImpl.Option option) {
        this.writeLock.lock();
        try {
            this.option = Objects.requireNonNull(option, "option");
            this.senderExecutorGroup = Objects.requireNonNull(option.getSenderExecutorGroup(), "senderExecutorGroup");
            this.logManager = Objects.requireNonNull(option.getLogManager(), "logManager");
            this.stateMachineCaller = Objects.requireNonNull(option.getStateMachineCaller(), "stateMachineCaller");
            this.ballotBox = Objects.requireNonNull(option.getBallotBox(), "ballotBox");
            this.replicaGroup = Objects.requireNonNull(option.getReplicaGroup(), "replicaGroup");
            this.configurationClient = Objects.requireNonNull(option.getConfigurationClient(), "configurationClient");
            this.fileService = Objects.requireNonNull(option.getFileService(), "fileService");
            this.state = STATE_SHUTDOWN;
        } finally {
            this.writeLock.unlock();
        }

    }

    @Override
    public void startup() {
        this.writeLock.lock();
        try {
            if (this.state == STATE_SHUTDOWN) {

                this.state = STATE_STARTED;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            if (this.state == STATE_STARTED) {
                List<ReplicaId> allSender = new ArrayList<>();
                allSender.addAll(this.senderContainer.keySet());
                allSender.forEach(this::removeAndShutdown);
                this.state = STATE_SHUTDOWN;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    void removeAndShutdown(ReplicaId replicaId) {
        final Sender sender = this.senderContainer.remove(replicaId);
        if (sender != null) {
            sender.shutdown();
        }
    }


    private boolean isStarted() {
        return this.state == STATE_STARTED;
    }

    private void ensureStarted() {
        if (!isStarted()) {
            throw new PacificaException(String.format("%s not started", this));
        }
    }

    @Override
    public String toString() {
        return "SenderGroupImpl{" +
                "state=" + state +
                '}';
    }

    public class SenderImplFactory implements SenderFactory {

        @Override
        public Sender getSender(ReplicaId targetId, final SenderType type) {
            SenderImpl sender = new SenderImpl(SenderGroupImpl.this.replica.getReplicaId(), targetId, type);
            SenderImpl.Option senderOption = new SenderImpl.Option();
            senderOption.setSenderExecutor(senderExecutorGroup.chooseExecutor());
            senderOption.setLogManager(logManager);
            senderOption.setStateMachineCaller(stateMachineCaller);
            senderOption.setBallotBox(ballotBox);
            senderOption.setPacificaClient(pacificaClient);
            senderOption.setReplicaGroup(replicaGroup);
            senderOption.setConfigurationClient(configurationClient);
            senderOption.setFileService(fileService);

            senderOption.setHeartbeatTimeoutMs(Math.max(100, option.getLeasePeriodTimeOutMs() / option.getHeartBeatFactor()));
            senderOption.setHeartBeatTimer(null);
            sender.init(senderOption);
            return sender;
        }
    }

    public static class Option {
        private LogManager logManager;
        private StateMachineCaller stateMachineCaller;
        private ExecutorGroup senderExecutorGroup;
        private BallotBox ballotBox;
        private ReplicaGroup replicaGroup;

        private FileService fileService;

        private ConfigurationClient configurationClient;

        private int leasePeriodTimeOutMs;

        private int heartBeatFactor = 30;

        public ConfigurationClient getConfigurationClient() {
            return configurationClient;
        }

        public void setConfigurationClient(ConfigurationClient configurationClient) {
            this.configurationClient = configurationClient;
        }

        public FileService getFileService() {
            return fileService;
        }

        public void setFileService(FileService fileService) {
            this.fileService = fileService;
        }

        public ReplicaGroup getReplicaGroup() {
            return replicaGroup;
        }

        public void setReplicaGroup(ReplicaGroup replicaGroup) {
            this.replicaGroup = replicaGroup;
        }

        public BallotBox getBallotBox() {
            return ballotBox;
        }

        public void setBallotBox(BallotBox ballotBox) {
            this.ballotBox = ballotBox;
        }

        public ExecutorGroup getSenderExecutorGroup() {
            return senderExecutorGroup;
        }

        public void setSenderExecutorGroup(ExecutorGroup senderExecutorGroup) {
            this.senderExecutorGroup = senderExecutorGroup;
        }

        public int getLeasePeriodTimeOutMs() {
            return leasePeriodTimeOutMs;
        }

        public void setLeasePeriodTimeOutMs(int leasePeriodTimeOutMs) {
            this.leasePeriodTimeOutMs = leasePeriodTimeOutMs;
        }

        public int getHeartBeatFactor() {
            return heartBeatFactor;
        }

        public void setHeartBeatFactor(int heartBeatFactor) {
            this.heartBeatFactor = heartBeatFactor;
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
