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

package com.trs.pacifica.example.counter.rpc;

import com.google.protobuf.Message;
import com.trs.pacifica.example.counter.CounterRpc;
import com.trs.pacifica.example.counter.replica.CounterClosure;
import com.trs.pacifica.example.counter.service.CounterService;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.RpcContext;
import com.trs.pacifica.rpc.RpcHandler;

public class IncrementAndGetRequestHandler implements RpcHandler<CounterRpc.IncrementAndGetRequest, CounterRpc.IncrementAndGetResponse> {

    private final CounterService counterService;


    public IncrementAndGetRequestHandler(CounterService counterService) {
        this.counterService = counterService;
    }


    @Override
    public void handleRequest(RpcContext<CounterRpc.IncrementAndGetResponse> rpcContext, CounterRpc.IncrementAndGetRequest request) {
        final long delta = request.getDelta();
        final String groupName = request.getGroupName();
        final String nodeId = request.getNodeId();
        CounterClosure<Long> done = new RpcCounterClosure<Long, CounterRpc.IncrementAndGetResponse>(rpcContext) {
            @Override
            public CounterRpc.IncrementAndGetResponse buildRpcResponse(Long result) {
                return CounterRpc.IncrementAndGetResponse.newBuilder()//
                        .setValue(result)//
                        .build();
            }
        };
        this.counterService.incrementAndGet(new ReplicaId(groupName, nodeId), delta, done);
    }

    @Override
    public String interest() {
        return CounterRpc.IncrementAndGetRequest.class.getName();
    }

}
