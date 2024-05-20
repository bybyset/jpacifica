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

import com.trs.pacifica.core.ReplicaImpl;
import com.trs.pacifica.core.ReplicaOption;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.RpcServer;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.EndpointFactory;

public class ReplicaWharf {

    private ReplicaId replicaId;
    private RpcClient rpcClient = null;
    private RpcServer rpcServer = null;
    private EndpointFactory endpointFactory = null;
    private ConfigurationClient configurationClient = null;
    private StateMachine stateMachine = null;

    private ReplicaImpl replicaImpl = null;


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


        private Builder(ReplicaId replicaId) {
            this.replicaId = replicaId;
        }

        public ReplicaWharf.Builder replicaOption(final ReplicaOption replicaOption) {
            this.replicaOption = replicaOption;
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
            return new ReplicaWharf();
        }
    }

}
