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
import com.trs.pacifica.async.thread.ReplicaExecutorGroupHolder;
import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.core.ReplicaOption;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.log.codec.LogEntryCodecFactory;
import com.trs.pacifica.log.codec.LogEntryCodecFactoryHolder;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.RpcServer;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.EndpointFactory;

public class ReplicaWharf {

    private final RpcClient rpcClient;
    private final RpcServer rpcServer;
    private final EndpointFactory endpointFactory;
    private final ConfigurationClient configurationClient;
    private final StateMachine stateMachine;
    private final ReplicaImpl replicaImpl;
    private final ReplicaOption replicaOption;

    public ReplicaWharf(RpcClient rpcClient, RpcServer rpcServer, EndpointFactory endpointFactory, ConfigurationClient configurationClient, StateMachine stateMachine, ReplicaImpl replicaImpl, ReplicaOption replicaOption) {
        this.rpcClient = rpcClient;
        this.rpcServer = rpcServer;
        this.endpointFactory = endpointFactory;
        this.configurationClient = configurationClient;
        this.stateMachine = stateMachine;
        this.replicaImpl = replicaImpl;
        this.replicaOption = replicaOption;
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
        private ReplicaOption replicaOption = new ReplicaOption();
        private RpcClient rpcClient = null;
        private RpcServer rpcServer = null;
        private EndpointFactory endpointFactory = null;
        private ConfigurationClient configurationClient = null;
        private StateMachine stateMachine = null;


        /****************************ReplicaOption************************************/
        private int gracePeriodTimeoutMs = ReplicaOption.DEFAULT_GRACE_PERIOD_TIMEOUT_MS;

        private int snapshotTimeoutMs = ReplicaOption.DEFAULT_SNAPSHOT_TIMEOUT_MS;

        private int recoverTimeoutMs = ReplicaOption.DEFAULT_RECOVER_TIMEOUT_MS;

        private int snapshotLogIndexReserved = ReplicaOption.DEFAULT_SNAPSHOT_LOG_INDEX_RESERVED;

        private int snapshotLogIndexMargin = ReplicaOption.DEFAULT_SNAPSHOT_LOG_INDEX_MARGIN;

        private int maxOperationNumPerBatch = ReplicaOption.DEFAULT_MAX_OPERATION_NUM_PER_BATCH;

        private boolean enableLogEntryChecksum = ReplicaOption.DEFAULT_ENABLE_LOG_ENTRY_CHECKSUM;

        private String logStoragePath = null;

        private String snapshotStoragePath = null;

        private ExecutorGroup applyExecutorGroup = ReplicaExecutorGroupHolder.getDefaultInstance();

        private ExecutorGroup logManagerExecutorGroup = ReplicaExecutorGroupHolder.getDefaultInstance();

        private ExecutorGroup fsmCallerExecutorGroup = ReplicaExecutorGroupHolder.getDefaultInstance();

        private LogEntryCodecFactory logEntryCodecFactory = LogEntryCodecFactoryHolder.getInstance();


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

        public Builder endpointFactory(EndpointFactory endpointFactory) {
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

        public Builder logEntryCodecFactory(LogEntryCodecFactory logEntryCodecFactory) {
            this.logEntryCodecFactory = logEntryCodecFactory;
            return this;
        }

        public ReplicaWharf build() throws PacificaException {
            final ReplicaImpl replicaImpl = new ReplicaImpl(this.replicaId);

            if (this.replicaOption == null) {
                this.replicaOption = new ReplicaOption();
            }


            this.replicaOption.setConfigurationClient(this.configurationClient);

            this.replicaOption.setRpcClient(this.rpcClient);
            this.replicaOption.setEndpointFactory(this.endpointFactory);

            this.replicaOption.setStateMachine(this.stateMachine);

            replicaImpl.init(replicaOption);
            return null;
        }
    }

}
