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

package com.trs.pacifica.example.counter.config;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.trs.pacifica.ConfigurationClient;
import com.trs.pacifica.example.counter.MetaReplicaRpc;
import com.trs.pacifica.example.counter.config.jraft.MasterServer;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CounterReplicaConfigClient implements ConfigurationClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CounterReplicaConfigClient.class);
    private final String groupId;
    private final CliClientServiceImpl cliClientService = new CliClientServiceImpl();

    public CounterReplicaConfigClient(final String groupId, final Configuration conf) {
        this.groupId = groupId;
        RouteTable.getInstance().updateConfiguration(groupId, conf);
        RpcOptions rpcOptions = new RpcOptions();
        cliClientService.init(rpcOptions);
    }

    public CounterReplicaConfigClient(final Configuration conf) {
        this(MasterServer.MASTER_GROUP_ID, conf);
    }

    @Override
    public ReplicaGroup getReplicaGroup(String groupName) {
        return null;
    }

    @Override
    public boolean addSecondary(long version, ReplicaId replicaId) {
        MetaReplicaRpc.AddSecondaryRequest request = MetaReplicaRpc.AddSecondaryRequest.newBuilder()
                .setGroupName(replicaId.getGroupName())//
                .setNodeId(replicaId.getNodeId())//
                .setVersion(version)//
                .build();
        try {
            MetaReplicaRpc.AddSecondaryResponse response = (MetaReplicaRpc.AddSecondaryResponse) sendRequest(request);
            return response.getSuccess();
        } catch (Throwable e) {
            LOGGER.error("failed to add secondary", e);
        }
        return false;
    }

    @Override
    public boolean removeSecondary(long version, ReplicaId replicaId) {
        MetaReplicaRpc.RemoveSecondaryRequest request = MetaReplicaRpc.RemoveSecondaryRequest.newBuilder()
                .setGroupName(replicaId.getGroupName())//
                .setNodeId(replicaId.getNodeId())//
                .setVersion(version)//
                .build();
        try {
            MetaReplicaRpc.RemoveSecondaryResponse response = (MetaReplicaRpc.RemoveSecondaryResponse) sendRequest(request);
            return response.getSuccess();
        } catch (Throwable e) {
            LOGGER.error("failed to add secondary", e);
        }
        return false;
    }

    @Override
    public boolean changePrimary(long version, ReplicaId replicaId) {
        MetaReplicaRpc.ChangePrimaryRequest request = MetaReplicaRpc.ChangePrimaryRequest.newBuilder()
                .setGroupName(replicaId.getGroupName())//
                .setNodeId(replicaId.getNodeId())//
                .setVersion(version)//
                .build();
        try {
            MetaReplicaRpc.ChangePrimaryResponse response = (MetaReplicaRpc.ChangePrimaryResponse) sendRequest(request);
            return response.getSuccess();
        } catch (Throwable e) {
            LOGGER.error("failed to add secondary", e);
        }
        return false;
    }

    private Object sendRequest(Object request) throws RemotingException, InterruptedException {
       return sendRequest(request, 50000);
    }

    private Object sendRequest(Object request, long timeoutMs) throws RemotingException, InterruptedException {
        PeerId leader = getLeader();
        Object response = this.cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), request, timeoutMs);
        if (response instanceof MetaReplicaRpc.ErrorResponse) {
            MetaReplicaRpc.ErrorResponse errorResponse = (MetaReplicaRpc.ErrorResponse) response;
            throw new RemotingException(String.format("remoting exception, code:[%d], msg:[%s]", errorResponse.getCode(), errorResponse.getMsg()));
        }
        return response;
    }

    PeerId getLeader() {
        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        return leader;
    }
}
