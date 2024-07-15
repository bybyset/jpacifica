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

public class CounterServerBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(CounterServerBootstrap.class);

    public static final String MASTER_DATA_DIR_NAME = "master";

    public static final String COUNTER_REPLICA_DIR_NAME = "counter";

    public static final String COUNTER_GROUP_NAME = "example_counter";


    public static void main(String[] args) {
        if (args.length != 6) {
            System.out
                    .println("Usage : java com.trs.pacifica.example.counter.CounterServerBootstrap {dataPath} {masterServerId} {masterInitConf} {nodeId} {serverAddress} {serverConfStr}");
            System.out
                    .println("Example: java com.trs.pacifica.example.counter.CounterServerBootstrap /tmp/server1 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083 node01 127.0.0.1:9091 node01:127.0.0.1:9091,node02:127.0.0.1:9092,node03:127.0.0.1:9093");
            System.exit(1);
        }
        LOGGER.debug("start counter example.");
        MasterServer masterServer = null;
        try {
            final String dataPath = args[0];
            final String masterServerIdStr = args[1];
            final String masterInitConfStr = args[2];

            final String nodeId = args[3];
            final String replicaServerAddress = args[4];
            final String replicaInitConfStr = args[5];

            // master server
            String masterDataPath = dataPath + File.separator + MASTER_DATA_DIR_NAME;
            final PeerId masterId = new PeerId();
            if (!masterId.parse(masterServerIdStr)) {
                throw new IllegalArgumentException("Fail to parse masterId:" + masterServerIdStr);
            }
            final Configuration initConf = new Configuration();
            if (!initConf.parse(masterInitConfStr)) {
                throw new IllegalArgumentException("Fail to parse initConf:" + masterInitConfStr);
            }

            FileUtils.forceMkdir(new File(masterDataPath));

            masterServer = new MasterServer(masterDataPath, masterId, initConf);
            CounterReplicaConfigClient counterReplicaConfigClient = new CounterReplicaConfigClient(initConf);

            // counter replica
            String counterReplicaDataPath = dataPath + File.separator + COUNTER_REPLICA_DIR_NAME;
            FileUtils.forceMkdir(new File(counterReplicaDataPath));
            final PeerId serverId = new PeerId();
            if (!serverId.parse(replicaServerAddress)) {
                throw new IllegalArgumentException("Fail to parse serverId:" + replicaServerAddress);
            }

            // 模拟节点注册发现, 你需要实现自己的注册发现逻辑
            registerNodeId(EndpointManagerHolder.getInstance(), replicaInitConfStr);
            Endpoint endpoint = new Endpoint(serverId.getIp(), serverId.getPort());
            GrpcClient rpcClient = (GrpcClient) JPacificaRpcServerFactory.createPacificaRpcClient();
            rpcClient.init(new GrpcClient.Option());
            rpcClient.startup();
            RpcServer rpcServer = JPacificaRpcServerFactory.createPacificaRpcServer(endpoint);
            CounterService counterService = new CounterServiceImpl(nodeId);
            startRpcServer(rpcServer, counterService);

            ReplicaId replicaId = new ReplicaId(COUNTER_GROUP_NAME, nodeId);
            counterReplicaConfigClient.addReplica(replicaId);
            CounterReplica counterReplica = new CounterReplica(counterReplicaConfigClient, replicaId, counterReplicaDataPath, rpcServer, rpcClient);
            counterReplica.start();
            ReplicaServiceManagerHolder.getInstance().registerReplicaService(replicaId, counterReplica.getReplicaService());

            //join
            for (;;) {
                Thread.sleep(1000);
                System.out.println("running....");
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (masterServer != null) {
                masterServer.shutdown();
            }
        }

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

    static void startRpcServer(RpcServer rpcServer, CounterService counterService) throws PacificaException {
        if (rpcServer instanceof GrpcServer) {
            GrpcServer grpcServer = (GrpcServer) rpcServer;
            grpcServer.extendMarshaller(CounterRpc.IncrementAndGetRequest.getDefaultInstance(), CounterRpc.IncrementAndGetResponse.getDefaultInstance());
        }
        rpcServer.registerRpcHandler(new IncrementAndGetRequestHandler(counterService));
        rpcServer.init(null);
        rpcServer.startup();
    }







}
