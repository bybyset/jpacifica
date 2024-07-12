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

package com.trs.pacifica;

import com.trs.pacifica.async.thread.ExecutorGroup;
import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.core.ReplicaOption;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.fs.FileServiceFactory;
import com.trs.pacifica.log.codec.LogEntryCodecFactory;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.RpcServer;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.EndpointManager;
import com.trs.pacifica.util.timer.TimerFactory;

import java.util.concurrent.Executor;

/**
 * Starting the bootstrap class
 */
public class ReplicaWharf {

    private final RpcClient rpcClient;
    private final RpcServer rpcServer;
    private final EndpointManager endpointFactory;
    private final ConfigurationClient configurationClient;
    private final StateMachine stateMachine;
    private final ReplicaImpl replicaImpl;
    private final ReplicaOption replicaOption;

    public ReplicaWharf(ReplicaImpl replicaImpl, ReplicaOption replicaOption, RpcClient rpcClient, RpcServer rpcServer, EndpointManager endpointFactory, ConfigurationClient configurationClient, StateMachine stateMachine) {
        this.rpcClient = rpcClient;
        this.rpcServer = rpcServer;
        this.endpointFactory = endpointFactory;
        this.configurationClient = configurationClient;
        this.stateMachine = stateMachine;
        this.replicaImpl = replicaImpl;
        this.replicaOption = replicaOption;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public RpcServer getRpcServer() {
        return rpcServer;
    }

    public EndpointManager getEndpointFactory() {
        return endpointFactory;
    }

    public ConfigurationClient getConfigurationClient() {
        return configurationClient;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public ReplicaImpl getReplicaImpl() {
        return replicaImpl;
    }

    public ReplicaOption getReplicaOption() {
        return replicaOption;
    }

    public void start() throws PacificaException {
        this.replicaImpl.startup();
    }

    public void shutdown() throws PacificaException {
        this.replicaImpl.shutdown();
    }


    public static Builder newBuilder(ReplicaId replicaId) {
        return new Builder(replicaId);
    }

    public static class Builder {
        private final ReplicaId replicaId;
        private final ReplicaOption replicaOption = new ReplicaOption();
        private RpcClient rpcClient = null;
        private RpcServer rpcServer = null;
        private EndpointManager endpointFactory = null;
        private ConfigurationClient configurationClient = null;
        private StateMachine stateMachine = null;


        /****************************ReplicaOption************************************/
        private Integer gracePeriodTimeoutMs = null;

        private Integer snapshotTimeoutMs = null;

        private Integer recoverTimeoutMs = null;

        private Integer snapshotLogIndexReserved = null;

        private Integer snapshotLogIndexMargin = null;

        private Integer maxOperationNumPerBatch = null;

        private Integer downloadSnapshotTimeoutMs = null;

        private Boolean enableLogEntryChecksum = null;

        private String logStoragePath = null;

        private String snapshotStoragePath = null;

        private ExecutorGroup applyExecutorGroup = null;

        private ExecutorGroup logManagerExecutorGroup = null;

        private ExecutorGroup fsmCallerExecutorGroup = null;
        private ExecutorGroup senderExecutorGroup = null;

        private Executor downloadSnapshotExecutor = null;

        private LogEntryCodecFactory logEntryCodecFactory = null;

        private FileServiceFactory fileServiceFactory = null;

        private TimerFactory timerFactory = null;

        private PacificaServiceFactory pacificaServiceFactory = null;


        private Builder(ReplicaId replicaId) {
            this.replicaId = replicaId;
        }

        public Builder rpcClient(RpcClient rpcClient) {
            this.rpcClient = rpcClient;
            return this;
        }

        public Builder rpcServer(RpcServer rpcServer) {
            this.rpcServer = rpcServer;
            return this;
        }

        public Builder endpointFactory(EndpointManager endpointFactory) {
            this.endpointFactory = endpointFactory;
            return this;
        }

        public Builder configurationClient(ConfigurationClient configurationClient) {
            this.configurationClient = configurationClient;
            return this;
        }

        public Builder stateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        public Builder gracePeriodTimeoutMs(int gracePeriodTimeoutMs) {
            this.gracePeriodTimeoutMs = gracePeriodTimeoutMs;
            return this;
        }

        public Builder snapshotTimeoutMs(int snapshotTimeoutMs) {
            this.snapshotTimeoutMs = snapshotTimeoutMs;
            return this;
        }

        public Builder recoverTimeoutMs(int recoverTimeoutMs) {
            this.recoverTimeoutMs = recoverTimeoutMs;
            return this;
        }

        public Builder snapshotLogIndexReserved(int snapshotLogIndexReserved) {
            this.snapshotLogIndexReserved = snapshotLogIndexReserved;
            return this;
        }

        public Builder snapshotLogIndexMargin(int snapshotLogIndexMargin) {
            this.snapshotLogIndexMargin = snapshotLogIndexMargin;
            return this;
        }

        public Builder maxOperationNumPerBatch(int maxOperationNumPerBatch) {
            this.maxOperationNumPerBatch = maxOperationNumPerBatch;
            return this;
        }

        public Builder downloadSnapshotTimeoutMs(int downloadSnapshotTimeoutMs) {
            this.downloadSnapshotTimeoutMs = downloadSnapshotTimeoutMs;
            return this;
        }

        public Builder logStoragePath(String logStoragePath) {
            this.logStoragePath = logStoragePath;
            return this;
        }

        public Builder snapshotStoragePath(String snapshotStoragePath) {
            this.snapshotStoragePath = snapshotStoragePath;
            return this;
        }

        public Builder applyExecutorGroup(ExecutorGroup applyExecutorGroup) {
            this.applyExecutorGroup = applyExecutorGroup;
            return this;
        }

        public Builder logManagerExecutorGroup(ExecutorGroup logManagerExecutorGroup) {
            this.logManagerExecutorGroup = logManagerExecutorGroup;
            return this;
        }

        public Builder fsmCallerExecutorGroup(ExecutorGroup fsmCallerExecutorGroup) {
            this.fsmCallerExecutorGroup = fsmCallerExecutorGroup;
            return this;
        }

        public Builder senderExecutorGroup(ExecutorGroup senderExecutorGroup) {
            this.senderExecutorGroup = senderExecutorGroup;
            return this;
        }

        public Builder downloadSnapshotExecutor(Executor downloadSnapshotExecutor) {
            this.downloadSnapshotExecutor = downloadSnapshotExecutor;
            return this;
        }

        public Builder logEntryCodecFactory(LogEntryCodecFactory logEntryCodecFactory) {
            this.logEntryCodecFactory = logEntryCodecFactory;
            return this;
        }

        public Builder fileServiceFactory(FileServiceFactory fileServiceFactory) {
            this.fileServiceFactory = fileServiceFactory;
            return this;
        }

        public Builder timerFactory(TimerFactory timerFactory) {
            this.timerFactory = timerFactory;
            return this;
        }

        public Builder pacificaServiceFactory(PacificaServiceFactory pacificaServiceFactory) {
            this.pacificaServiceFactory = pacificaServiceFactory;
            return this;
        }


        public ReplicaWharf build() throws PacificaException {
            final ReplicaImpl replicaImpl = new ReplicaImpl(this.replicaId);

            if (this.configurationClient != null) {
                this.replicaOption.setConfigurationClient(configurationClient);
            }
            if (this.rpcClient != null) {
                this.replicaOption.setRpcClient(rpcClient);
            }
            if (this.endpointFactory != null) {
                this.replicaOption.setEndpointFactory(endpointFactory);
            }
            if (this.stateMachine != null) {
                this.replicaOption.setStateMachine(stateMachine);
            }
            if (this.gracePeriodTimeoutMs != null) {
                this.replicaOption.setGracePeriodTimeoutMs(gracePeriodTimeoutMs);
            }
            if (this.snapshotTimeoutMs != null) {
                this.replicaOption.setSnapshotTimeoutMs(snapshotTimeoutMs);
            }
            if (this.recoverTimeoutMs != null) {
                this.replicaOption.setRecoverTimeoutMs(recoverTimeoutMs);
            }
            if (this.snapshotLogIndexReserved != null) {
                this.replicaOption.setSnapshotLogIndexReserved(snapshotLogIndexReserved);
            }
            if (this.snapshotLogIndexMargin != null) {
                this.replicaOption.setSnapshotLogIndexMargin(snapshotLogIndexMargin);
            }
            if (this.maxOperationNumPerBatch != null) {
                this.replicaOption.setMaxOperationNumPerBatch(maxOperationNumPerBatch);
            }
            if (this.downloadSnapshotTimeoutMs != null) {
                this.replicaOption.setDownloadSnapshotTimeoutMs(downloadSnapshotTimeoutMs);
            }
            if (this.enableLogEntryChecksum != null) {
                this.replicaOption.setEnableLogEntryChecksum(enableLogEntryChecksum);
            }
            if (this.logStoragePath != null) {
                this.replicaOption.setLogStoragePath(logStoragePath);
            }
            if (this.snapshotStoragePath != null) {
                this.replicaOption.setSnapshotStoragePath(snapshotStoragePath);
            }

            if (this.applyExecutorGroup != null) {
                this.replicaOption.setApplyExecutorGroup(applyExecutorGroup);
            }
            if (this.logManagerExecutorGroup != null) {
                this.replicaOption.setLogManagerExecutorGroup(logManagerExecutorGroup);
            }
            if (this.fsmCallerExecutorGroup != null) {
                this.replicaOption.setFsmCallerExecutorGroup(fsmCallerExecutorGroup);
            }
            if (this.senderExecutorGroup != null) {
                this.replicaOption.setSenderExecutorGroup(senderExecutorGroup);
            }
            if (this.downloadSnapshotExecutor != null) {
                this.replicaOption.setDownloadSnapshotExecutor(downloadSnapshotExecutor);
            }
            if (this.logEntryCodecFactory != null) {
                this.replicaOption.setLogEntryCodecFactory(logEntryCodecFactory);
            }
            if (this.fileServiceFactory != null) {
                this.replicaOption.setFileServiceFactory(fileServiceFactory);
            }
            if (this.timerFactory != null) {
                this.replicaOption.setTimerFactory(timerFactory);
            }
            if (this.pacificaServiceFactory != null) {
                this.replicaOption.setPacificaServiceFactory(pacificaServiceFactory);
            }

            replicaImpl.init(replicaOption);

            return new ReplicaWharf(replicaImpl,//
                    this.replicaOption, //
                    this.rpcClient, //
                    this.rpcServer, //
                    this.endpointFactory, //
                    this.configurationClient, //
                    this.stateMachine);
        }
    }

}
