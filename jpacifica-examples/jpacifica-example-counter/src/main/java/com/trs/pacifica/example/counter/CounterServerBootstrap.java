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
import com.trs.pacifica.example.counter.config.CounterGrpcHelper;
import com.trs.pacifica.example.counter.config.CounterReplicaConfigClient;
import com.trs.pacifica.example.counter.config.jraft.MasterServer;
import com.trs.pacifica.rpc.node.Endpoint;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CounterServerBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(CounterServerBootstrap.class);

    public static final String MASTER_DATA_DIR_NAME = "master";

    public static final String COUNTER_REPLICA_DIR_NAME = "counter";


    static MasterServer masterServer = null;

    static CounterServer counterServer = null;


    public static void main(String[] args) {
        if (args.length != 6) {
            System.out
                    .println("Usage : java com.trs.pacifica.example.counter.CounterServerBootstrap {dataPath} {masterServerId} {masterInitConf} {nodeId} {serverAddress} {serverConfStr}");
            System.out
                    .println("Example: java com.trs.pacifica.example.counter.CounterServerBootstrap /tmp/server1 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083 node01 127.0.0.1:9091 node01:127.0.0.1:9091,node02:127.0.0.1:9092,node03:127.0.0.1:9093");
            System.exit(1);
        }
        LOGGER.debug("start counter example.");
        try {
            final String dataPath = args[0];
            final String masterServerIdStr = args[1];
            final String masterInitConfStr = args[2];
            final String nodeId = args[3];
            final String replicaServerAddress = args[4];
            final String replicaInitConfStr = args[5];
            // master server
            final String masterDataPath = dataPath + File.separator + MASTER_DATA_DIR_NAME;
            final PeerId masterId = new PeerId();
            if (!masterId.parse(masterServerIdStr)) {
                throw new IllegalArgumentException("Fail to parse masterId:" + masterServerIdStr);
            }
            final Configuration initConf = new Configuration();
            if (!initConf.parse(masterInitConfStr)) {
                throw new IllegalArgumentException("Fail to parse initConf:" + masterInitConfStr);
            }
            final PeerId serverId = new PeerId();
            if (!serverId.parse(replicaServerAddress)) {
                throw new IllegalArgumentException("Fail to parse serverId:" + replicaServerAddress);
            }
            CounterGrpcHelper.registerProtobufSerializer();
            if (initConf.contains(masterId)) {
                MasterServer.Option masterOption = new MasterServer.Option();
                masterOption.setDataPath(masterDataPath);
                masterOption.setServerId(masterId);
                masterOption.setConfiguration(initConf);
                masterServer = new MasterServer();
                masterServer.init(masterOption);
                masterServer.startup();
            }

            String counterReplicaDataPath = dataPath + File.separator + COUNTER_REPLICA_DIR_NAME;
            FileUtils.forceMkdir(new File(counterReplicaDataPath));
            Endpoint endpoint = new Endpoint(serverId.getIp(), serverId.getPort());
            CounterReplicaConfigClient counterReplicaConfigClient = new CounterReplicaConfigClient(initConf);
            CounterServer.Option serverOption = new CounterServer.Option();
            serverOption.setCounterReplicaConfigClient(counterReplicaConfigClient);
            serverOption.setNodeId(nodeId);
            serverOption.setDataPath(counterReplicaDataPath);
            serverOption.setServerAddress(endpoint);
            serverOption.setReplicaInitConfStr(replicaInitConfStr);
            counterServer = new CounterServer();
            counterServer.init(serverOption);
            counterServer.startup();

            //join
            for (;;) {
                Thread.sleep(1000);
                System.out.println("running....");
            }
        } catch (Throwable e) {
            LOGGER.error("failed to start", e);
        } finally {
            if (masterServer != null) {
                try {
                    masterServer.shutdown();
                } catch (Throwable throwable) {

                }
            }
            if (counterServer != null) {
                try {
                    counterServer.shutdown();
                } catch (Throwable throwable) {
                }
            }
        }

    }









}
