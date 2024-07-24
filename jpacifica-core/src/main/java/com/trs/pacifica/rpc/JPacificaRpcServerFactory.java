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

package com.trs.pacifica.rpc;

import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.internal.*;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.rpc.service.ReplicaServiceManager;
import com.trs.pacifica.rpc.service.ReplicaServiceManagerHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * RpcServer Factory class
 */
public class JPacificaRpcServerFactory {

    static final Logger LOGGER = LoggerFactory.getLogger(JPacificaRpcServerFactory.class);

    private JPacificaRpcServerFactory() {
    }


    /**
     * create RpcClient
     * @return
     */
    public static RpcClient createPacificaRpcClient() {
        return RpcFactoryHolder.getInstance().createRpcClient();
    }


    public static RpcServer createPacificaRpcServer(final Endpoint endpoint) {
        final RpcServer rpcServer = RpcFactoryHolder.getInstance().createRpcServer(endpoint);
        if (rpcServer != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("create RpcServer={}", rpcServer.getClass().getName());
            }
            ReplicaServiceManager replicaManager = ReplicaServiceManagerHolder.getInstance();
            addPacificaRequestHandlers(rpcServer, replicaManager);
        }
        return rpcServer;
    }


    /**
     * create RpcServer for pacifica.
     *
     * @param endpoint              address of the node
     * @param replicaServiceManager
     * @return
     */
    public static RpcServer createPacificaRpcServer(final Endpoint endpoint, final ReplicaServiceManager replicaServiceManager) {
        final RpcServer rpcServer = RpcFactoryHolder.getInstance().createRpcServer(endpoint);
        if (rpcServer != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("create RpcServer={}", rpcServer.getClass().getName());
            }
            addPacificaRequestHandlers(rpcServer, replicaServiceManager);
        }
        return rpcServer;
    }

    /**
     * add default RequestHandler for rpcServer
     *
     * @param rpcServer
     * @param replicaServiceManager
     */
    public static void addPacificaRequestHandlers(final RpcServer rpcServer, final ReplicaServiceManager replicaServiceManager) {
        Objects.requireNonNull(rpcServer, "rpcServer");
        Objects.requireNonNull(replicaServiceManager, "replicaServiceManager");
        rpcServer.registerRpcHandler(new AppendEntriesRequestHandler(replicaServiceManager));
        rpcServer.registerRpcHandler(new ReplicaRecoverRequestHandler(replicaServiceManager));
        rpcServer.registerRpcHandler(new InstallSnapshotRequestHandler(replicaServiceManager));
        rpcServer.registerRpcHandler(new GetFileRequestHandler(replicaServiceManager));
        rpcServer.registerRpcHandler(new PingReplicaRequestHandler(replicaServiceManager));
    }


}
