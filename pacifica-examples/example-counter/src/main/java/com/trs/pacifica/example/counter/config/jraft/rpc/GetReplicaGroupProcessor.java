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

package com.trs.pacifica.example.counter.config.jraft.rpc;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.google.protobuf.Message;
import com.trs.pacifica.example.counter.MetaReplicaRpc;
import com.trs.pacifica.example.counter.config.jraft.MetaReplicaService;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;

public class GetReplicaGroupProcessor implements RpcProcessor<MetaReplicaRpc.GetReplicaGroupRequest> {

    private final MetaReplicaService metaReplicaService;

    public GetReplicaGroupProcessor(MetaReplicaService metaReplicaService) {
        this.metaReplicaService = metaReplicaService;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, MetaReplicaRpc.GetReplicaGroupRequest request) {
        RpcMetaReplicaClosure<ReplicaGroup> closure = new RpcMetaReplicaClosure<ReplicaGroup>(rpcCtx) {
            @Override
            public Message buildRpcResponse(ReplicaGroup result) {
                if (result != null) {
                    MetaReplicaRpc.GetReplicaGroupResponse.Builder builder = MetaReplicaRpc.GetReplicaGroupResponse.newBuilder()//
                            .setGroupName(result.getGroupName())//
                            .setVersion(result.getVersion())//
                            .setTerm(result.getPrimaryTerm())//
                            .setPrimary(result.getPrimary().getNodeId());
                    for (ReplicaId replicaId : result.listSecondary()) {
                        builder.addSecondary(replicaId.getNodeId());
                    }
                    return builder.build();
                }
                return MetaReplicaRpc.GetReplicaGroupResponse.newBuilder().build();
            }
        };
        final String groupName = request.getGroupName();

        metaReplicaService.getReplicaGroup(groupName, closure);
    }

    @Override
    public String interest() {
        return MetaReplicaRpc.GetReplicaGroupRequest.class.getName();
    }
}
