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

package com.trs.pacifica.example.counter;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.example.counter.config.CounterReplicaConfigClient;
import com.trs.pacifica.example.counter.config.jraft.MasterServer;
import com.trs.pacifica.example.counter.replica.CounterReplica;
import com.trs.pacifica.example.counter.rpc.IncrementAndGetRequestHandler;
import com.trs.pacifica.example.counter.service.CounterService;
import com.trs.pacifica.example.counter.service.CounterServiceImpl;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.JPacificaRpcServerFactory;
import com.trs.pacifica.rpc.RpcServer;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.impl.grpc.GrpcClient;
import com.trs.pacifica.rpc.impl.grpc.GrpcServer;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.rpc.node.EndpointManager;
import com.trs.pacifica.rpc.node.EndpointManagerHolder;
import com.trs.pacifica.rpc.service.ReplicaServiceManagerHolder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;

public class CounterServer implements LifeCycle<CounterServer.Option> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CounterServer.class);

    public static final String COUNTER_GROUP_NAME = "example_counter";
    private Option option = null;

    private MasterServer metaServer = null;

    private String dataPath = null;

    private CounterReplicaConfigClient counterReplicaConfigClient = null;

    private String nodeId = null;
    private Endpoint nodeAddress = null;
    private RpcServer rpcServer;
    private RpcClient rpcClient = null;

    private CounterReplica counterReplica;

    @Override
    public void init(Option option) throws PacificaException {
        this.option = Objects.requireNonNull(option, "option");
        this.dataPath = Objects.requireNonNull(option.getDataPath(), "dataPath");
        this.counterReplicaConfigClient = Objects.requireNonNull(option.getCounterReplicaConfigClient(), "counterReplicaConfigClient");
        this.nodeId = Objects.requireNonNull(option.getNodeId(), "nodeId");
        this.nodeAddress = Objects.requireNonNull(option.getServerAddress(), "nodeAddress");
        this.rpcServer = JPacificaRpcServerFactory.createPacificaRpcServer(nodeAddress);

    }

    @Override
    public void startup() throws PacificaException {
        try {
            // counter replica
            FileUtils.forceMkdir(new File(this.dataPath));

            registerNodeId(EndpointManagerHolder.getInstance(), Objects.requireNonNull(this.option.getReplicaInitConfStr(), ""));
            this.rpcClient = (GrpcClient) JPacificaRpcServerFactory.createPacificaRpcClient();
            CounterService counterService = new CounterServiceImpl(nodeId);
            startRpcServer(rpcServer, counterService);
            addReplica();
        } catch (Throwable e) {
            throw new PacificaException(PacificaErrorCode.IO, "", e);
        }

    }

    @Override
    public void shutdown() throws PacificaException {
        if (this.rpcServer != null) {
            rpcServer.shutdown();
        }
        if (this.counterReplica != null) {
            this.counterReplica.shutdown();
        }
    }


    private void addReplica() throws PacificaException {
        ReplicaId replicaId = new ReplicaId(COUNTER_GROUP_NAME, this.nodeId);
        if (counterReplicaConfigClient.addReplica(replicaId)) {
            LOGGER.debug("success to add meta replica, replicaId={}", replicaId);
            counterReplica = new CounterReplica(counterReplicaConfigClient, replicaId, this.dataPath, rpcServer, rpcClient);
            counterReplica.start();
            ReplicaServiceManagerHolder.getInstance().registerReplicaService(replicaId, counterReplica.getReplicaService());
        }
    }

    static void startRpcServer(RpcServer rpcServer, CounterService counterService) throws PacificaException {
        if (rpcServer instanceof GrpcServer) {
            GrpcServer grpcServer = (GrpcServer) rpcServer;
            grpcServer.extendMarshaller(CounterRpc.IncrementAndGetRequest.getDefaultInstance(), CounterRpc.IncrementAndGetResponse.getDefaultInstance());
        }
        rpcServer.registerRpcHandler(new IncrementAndGetRequestHandler(counterService));
        rpcServer.init(null);
        rpcServer.startup();
    }

    static void registerNodeId(EndpointManager endpointManager, String replicaInitConfStr) {
        // nodeId:ip:port;nodeId:ip:port;
        for (String nodeStr : StringUtils.split(replicaInitConfStr, ',')) {
            if (StringUtils.isNotBlank(nodeStr)) {
                String[] args = nodeStr.split(":");
                endpointManager.registerEndpoint(args[0], new Endpoint(args[1], Integer.parseInt(args[2])));
            }
        }
    }


    public static class Option {

        private String dataPath;
        private String nodeId;
        private Endpoint serverAddress;
        private CounterReplicaConfigClient counterReplicaConfigClient;

        private String replicaInitConfStr;


        public String getDataPath() {
            return dataPath;
        }

        public void setDataPath(String dataPath) {
            this.dataPath = dataPath;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public Endpoint getServerAddress() {
            return serverAddress;
        }

        public void setServerAddress(Endpoint serverAddress) {
            this.serverAddress = serverAddress;
        }

        public CounterReplicaConfigClient getCounterReplicaConfigClient() {
            return counterReplicaConfigClient;
        }

        public void setCounterReplicaConfigClient(CounterReplicaConfigClient counterReplicaConfigClient) {
            this.counterReplicaConfigClient = counterReplicaConfigClient;
        }

        public String getReplicaInitConfStr() {
            return replicaInitConfStr;
        }

        public void setReplicaInitConfStr(String replicaInitConfStr) {
            this.replicaInitConfStr = replicaInitConfStr;
        }
    }
}
