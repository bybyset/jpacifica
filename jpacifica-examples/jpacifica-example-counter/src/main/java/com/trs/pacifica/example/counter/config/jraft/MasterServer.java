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
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;
import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.example.counter.MetaReplicaRpc;
import com.trs.pacifica.example.counter.config.CounterGrpcHelper;
import com.trs.pacifica.example.counter.config.jraft.fsm.ReplicaFsm;
import com.trs.pacifica.example.counter.config.jraft.rpc.*;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Objects;

public class MasterServer implements LifeCycle<MasterServer.Option> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterServer.class);

    public static final String DEFAULT_GROUP_ID = "master";

    public static final String LOG_DIR_NAME = "wal";
    public static final String SNAPSHOT_DIR_NAME = "snapshot";
    public static final String RAFT_META_DIR_NAME = "raft_meta";


    private RaftGroupService raftGroupService;
    private Node node;
    private ReplicaFsm replicaFsm;
    private Option option = null;

    private MetaReplicaService metaReplicaService;
    private String groupId = DEFAULT_GROUP_ID;

    public MasterServer() {

    }

    @Override
    public void init(Option option) throws PacificaException {
        this.option = Objects.requireNonNull(option, "option");
        this.metaReplicaService = new MetaReplicaServiceImpl(this);
        this.groupId = Objects.requireNonNull(option.getGroupId(), "groupId");
    }

    @Override
    public void startup() throws PacificaException {
        try {
            final String dataPath = this.option.getDataPath();
            final PeerId masterServerId = this.option.getServerId();
            FileUtils.forceMkdir(new File(dataPath));
            final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(masterServerId.getEndpoint());
            startRpcServer(rpcServer);
            final Configuration configuration = this.option.getConfiguration();
            final NodeOptions options = new NodeOptions();
            this.replicaFsm = new ReplicaFsm();
            options.setInitialConf(configuration);
            options.setFsm(replicaFsm);
            options.setLogUri(dataPath + File.separator + LOG_DIR_NAME);
            options.setSnapshotUri(dataPath + File.separator + SNAPSHOT_DIR_NAME);
            options.setRaftMetaUri(dataPath + File.separator + RAFT_META_DIR_NAME);
            options.setSnapshotIntervalSecs(10 * 1000);
            this.raftGroupService = new RaftGroupService(groupId, masterServerId, options, rpcServer);
            this.node = raftGroupService.start();
        } catch (Throwable e) {
            throw new PacificaException(PacificaErrorCode.IO, "", e);
        }

    }

    @Override
    public void shutdown() throws PacificaException{
        this.getRaftGroupService().shutdown();
    }


    void startRpcServer(RpcServer rpcServer) {
        rpcServer.registerProcessor(new AddSecondaryProcessor(metaReplicaService));
        rpcServer.registerProcessor(new RemoveSecondaryProcessor(metaReplicaService));
        rpcServer.registerProcessor(new ChangePrimaryProcessor(metaReplicaService));
        rpcServer.registerProcessor(new GetReplicaGroupProcessor(metaReplicaService));
        rpcServer.registerProcessor(new AddReplicaProcessor(metaReplicaService));
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


    public static class Option {

        private String groupId = DEFAULT_GROUP_ID;
        private String dataPath;
        private PeerId serverId;
        private Configuration configuration;

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getDataPath() {
            return dataPath;
        }

        public void setDataPath(String dataPath) {
            this.dataPath = dataPath;
        }

        public PeerId getServerId() {
            return serverId;
        }

        public void setServerId(PeerId serverId) {
            this.serverId = serverId;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public void setConfiguration(Configuration configuration) {
            this.configuration = configuration;
        }
    }

}
