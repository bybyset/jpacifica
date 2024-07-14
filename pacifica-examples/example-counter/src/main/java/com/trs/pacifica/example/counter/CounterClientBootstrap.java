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

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.example.counter.config.CounterReplicaConfigClient;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.JPacificaRpcServerFactory;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.impl.grpc.GrpcClient;
import com.trs.pacifica.rpc.node.Endpoint;
import com.trs.pacifica.rpc.node.EndpointManager;
import com.trs.pacifica.rpc.node.EndpointManagerHolder;
import org.apache.commons.lang.StringUtils;

public class CounterClientBootstrap {


    public static void main(String[] args) {
        final String groupName = args[0];
        final String masterInitConfStr = args[1];
        final String replicaInitConfStr = args[2];

        if (args.length != 3) {
            System.out
                    .println("Usage : java com.trs.pacifica.example.counter.CounterClientBootstrap {groupName} {masterInitConf} {serverConfStr}");
            System.out
                    .println("Example: java com.trs.pacifica.example.counter.CounterClientBootstrap example_counter 127.0.0.1:8081 node01:127.0.0.1:9091,node02:127.0.0.1:9092,node03:127.0.0.1:9093");
            System.exit(1);
        }

        final Configuration initConf = new Configuration();
        if (!initConf.parse(masterInitConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + masterInitConfStr);
        }
        EndpointManager endpointManager = EndpointManagerHolder.getInstance();
        registerNodeId(endpointManager, replicaInitConfStr);
        CounterReplicaConfigClient counterReplicaConfigClient = new CounterReplicaConfigClient(initConf);

        final ReplicaGroup replicaGroup = counterReplicaConfigClient.getReplicaGroup(groupName);

        ReplicaId primaryId = replicaGroup.getPrimary();
        Endpoint primaryEndPoint = endpointManager.getEndpoint(primaryId.getNodeId());
        GrpcClient rpcClient = (GrpcClient) JPacificaRpcServerFactory.createPacificaRpcClient();
        rpcClient.extendMarshaller(CounterRpc.IncrementAndGetRequest.getDefaultInstance(), CounterRpc.IncrementAndGetResponse.getDefaultInstance());
        try {
            rpcClient.init(new GrpcClient.Option());
            rpcClient.startup();
            long delta = 3;
            for (int i = 0; i < 10; i++) {
                long value = incrementAndGet(rpcClient, primaryEndPoint, groupName, delta);
                System.out.println(value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    static long incrementAndGet(RpcClient rpcClient, Endpoint primary, String groupName, final long delta) throws PacificaException {

        CounterRpc.IncrementAndGetRequest request = CounterRpc.IncrementAndGetRequest.newBuilder()//
                .setDelta(delta)//
                .setGroupName(groupName)
                .build();

        final CounterRpc.IncrementAndGetResponse response = (CounterRpc.IncrementAndGetResponse) rpcClient.invokeSync(primary, request, 60 * 1000);
        return response.getValue();
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
}
