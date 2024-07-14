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

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.google.protobuf.Message;
import com.trs.pacifica.example.counter.MetaReplicaRpc;
import com.trs.pacifica.example.counter.config.jraft.MetaReplicaService;
import com.trs.pacifica.model.ReplicaId;

import java.util.Objects;

public class AddReplicaProcessor implements RpcProcessor<MetaReplicaRpc.AddReplicaRequest> {

    private final MetaReplicaService metaReplicaService;

    public AddReplicaProcessor(MetaReplicaService metaReplicaService) {
        this.metaReplicaService = metaReplicaService;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, MetaReplicaRpc.AddReplicaRequest request) {
        RpcMetaReplicaClosure<Boolean> closure = new RpcMetaReplicaClosure<Boolean>(rpcCtx) {
            @Override
            public Message buildRpcResponse(Boolean result) {
                return MetaReplicaRpc.AddReplicaResponse.newBuilder().setSuccess(result).build();
            }
        };
        try {
            final String groupName = Objects.requireNonNull(request.getGroupName(), "groupName");
            final String nodeId = Objects.requireNonNull(request.getNodeId(), "nodeId");
            final ReplicaId replicaId = new ReplicaId(groupName, nodeId);
            this.metaReplicaService.addReplica(replicaId, closure);
        } catch (Throwable e) {
            closure.run(new Status(RaftError.EREQUEST, e.getMessage()));
        }
    }

    @Override
    public String interest() {
        return MetaReplicaRpc.AddReplicaRequest.class.getName();
    }
}
