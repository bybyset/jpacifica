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

package com.trs.pacifica.example.counter.config.jraft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.trs.pacifica.example.counter.config.jraft.fsm.ReplicaFsm;
import com.trs.pacifica.example.counter.config.jraft.rpc.*;

import java.io.File;

public class MasterServer {

    public static final String DEFAULT_GROUP_ID = "master";

    public static final String LOG_DIR_NAME = "wal";
    public static final String SNAPSHOT_DIR_NAME = "snapshot";
    public static final String RAFT_META_DIR_NAME = "raft_meta";


    private RaftGroupService raftGroupService;
    private Node node;
    private ReplicaFsm replicaFsm;


    public MasterServer(String dataPath, PeerId serverId, Configuration configuration, String groupId, NodeOptions options) {
        if (options == null) {
            options = new NodeOptions();
        }
        MetaReplicaService metaReplicaService = new MetaReplicaServiceImpl(this);
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        rpcServer.registerProcessor(new AddSecondaryProcessor(metaReplicaService));
        rpcServer.registerProcessor(new RemoveSecondaryProcessor(metaReplicaService));
        rpcServer.registerProcessor(new ChangePrimaryProcessor(metaReplicaService));
        rpcServer.registerProcessor(new GetReplicaGroupProcessor(metaReplicaService));
        rpcServer.registerProcessor(new AddReplicaProcessor(metaReplicaService));
        this.replicaFsm = new ReplicaFsm();
        options.setInitialConf(configuration);
        options.setFsm(replicaFsm);
        options.setLogUri(dataPath + File.separator + LOG_DIR_NAME);
        options.setSnapshotUri(dataPath + File.separator + SNAPSHOT_DIR_NAME);
        options.setRaftMetaUri(dataPath + File.separator + RAFT_META_DIR_NAME);
        this.raftGroupService = new RaftGroupService(groupId, serverId, options, rpcServer);
        this.node = raftGroupService.start();
    }

    public MasterServer(String dataPath, PeerId serverId, Configuration configuration) {
        this(dataPath, serverId, configuration, DEFAULT_GROUP_ID, new NodeOptions());
    }

    public RaftGroupService getRaftGroupService() {
        return raftGroupService;
    }

    public Node getNode() {
        return node;
    }

    public ReplicaFsm getReplicaFsm() {
        return replicaFsm;
    }

    public void shutdown() {
        this.getRaftGroupService().shutdown();
    }

}
