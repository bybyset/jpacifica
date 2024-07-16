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

package com.trs.pacifica.example.counter.replica;

import com.trs.pacifica.ConfigurationClient;
import com.trs.pacifica.Replica;
import com.trs.pacifica.ReplicaWharf;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.example.counter.rpc.IncrementAndGetRequestHandler;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.JPacificaRpcServerFactory;
import com.trs.pacifica.rpc.RpcServer;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.rpc.service.ReplicaService;

import java.io.File;

public class CounterReplica {

    public static final String LOG_DIR_NAME = "wal";
    public static final String SNAPSHOT_DIR_NAME = "snapshot";

    private ReplicaWharf replicaWharf;

    private final ConfigurationClient configurationClient;
    private final ReplicaId replicaId;
    private final String dataDirPath;
    private final RpcServer rpcServer;
    private final RpcClient rpcClient;

    public CounterReplica(ConfigurationClient configurationClient, ReplicaId replicaId, String dataDirPath, RpcServer rpcServer, RpcClient rpcClient) {
        this.configurationClient = configurationClient;
        this.replicaId = replicaId;
        this.dataDirPath = dataDirPath;
        this.rpcServer = rpcServer;
        this.rpcClient = rpcClient;
    }


    public Replica getReplica() {
        if (replicaWharf != null) {
            return this.replicaWharf.getReplicaImpl();
        }
        return null;
    }

    public ReplicaService getReplicaService() {
        if (replicaWharf != null) {
            return this.replicaWharf.getReplicaImpl();
        }
        return null;
    }


    public void start() throws PacificaException {
        String logStoragePath = dataDirPath + File.separator + LOG_DIR_NAME;
        String snapshotStoragePath = dataDirPath + File.separator + SNAPSHOT_DIR_NAME;
        CounterFsm counterFsm = new CounterFsm();
        this.replicaWharf = ReplicaWharf.newBuilder(replicaId)//
                .stateMachine(counterFsm)//
                .logStoragePath(logStoragePath)//
                .snapshotStoragePath(snapshotStoragePath)//
                .configurationClient(this.configurationClient)//
                .rpcServer(rpcServer)//
                .rpcClient(rpcClient)//
                .build();

        this.replicaWharf.start();
    }

    public void shutdown() throws PacificaException {
        this.replicaWharf.shutdown();
    }


}
