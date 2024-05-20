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

import com.trs.pacifica.ReplicaManager;
import com.trs.pacifica.rpc.internal.*;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.util.SystemPropertyUtil;

import java.util.Objects;

public interface RpcServer {

    static final int DEFAULT_RPC_SERVER_MAX_INBOUND_MESSAGE_SIZE = 32 * 1024 * 1024;
    static final int RPC_SERVER_MAX_INBOUND_MESSAGE_SIZE = SystemPropertyUtil.getInt(
            "jpacifica.grpc.max.inbound.message.size.bytes", DEFAULT_RPC_SERVER_MAX_INBOUND_MESSAGE_SIZE);

    void registerRpcHandler(final RpcHandler<?, ?> rpcHandler);


    public static RpcServer createPacificaRpcServer(final Endpoint endpoint, final ReplicaManager replicaManager) {
        final RpcServer rpcServer = RpcFactoryHolder.getInstance().createRpcServer(endpoint);
        addPacificaRequestHandlers(rpcServer, replicaManager);
        return rpcServer;
    }

    public static void addPacificaRequestHandlers(final RpcServer rpcServer, final ReplicaManager replicaManager) {
        Objects.requireNonNull(rpcServer, "rpcServer");
        Objects.requireNonNull(replicaManager, "replicaManager");
        rpcServer.registerRpcHandler(new AppendEntriesRequestHandler(replicaManager));
        rpcServer.registerRpcHandler(new ReplicaRecoverRequestHandler(replicaManager));
        rpcServer.registerRpcHandler(new InstallSnapshotRequestHandler(replicaManager));
        rpcServer.registerRpcHandler(new GetFileRequestHandler(replicaManager));
        rpcServer.registerRpcHandler(new PingReplicaRequestHandler(replicaManager));
    }


}
