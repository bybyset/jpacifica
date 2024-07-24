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

package com.trs.pacifica.rpc.impl.core;

import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcContext;
import com.trs.pacifica.rpc.RpcRequestHandler;
import com.trs.pacifica.rpc.internal.AppendEntriesRequestHandler;
import com.trs.pacifica.rpc.service.DefaultReplicaServiceManager;
import com.trs.pacifica.rpc.service.ReplicaService;
import com.trs.pacifica.rpc.service.ReplicaServiceManager;
import com.trs.pacifica.util.RpcUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AppendEntriesRequestHandlerTest {


    private ReplicaId primaryId = new ReplicaId("test_group", "node1");

    private ReplicaId secondaryId = new ReplicaId("test_group", "node2");

    private ReplicaService replicaService;
    private ReplicaServiceManager replicaServiceManager = new DefaultReplicaServiceManager();


    private RpcContext<RpcRequest.AppendEntriesResponse> rpcContext;

    @BeforeEach
    public void setup() {
        this.replicaService = Mockito.mock(ReplicaService.class);
        this.rpcContext = Mockito.mock(RpcContext.class);
        this.replicaServiceManager.registerReplicaService(secondaryId, replicaService);
    }

    protected RpcRequestHandler<RpcRequest.AppendEntriesRequest, RpcRequest.AppendEntriesResponse> newHandler() {
        return new AppendEntriesRequestHandler(this.replicaServiceManager);
    }

    protected RpcRequest.AppendEntriesRequest createRequest() {
        return RpcRequest.AppendEntriesRequest.newBuilder()//
                .setPrimaryId(RpcUtil.protoReplicaId(primaryId))//
                .setTargetId(RpcUtil.protoReplicaId(secondaryId))//
                .setPrevLogIndex(0)//
                .setPrevLogTerm(1)//
                .setVersion(1)//
                .setTerm(1)//
                .setCommitPoint(10)//
                .build();
    }


    public void verify(String interest, ReplicaService service, RpcRequestHandler<RpcRequest.AppendEntriesRequest, RpcRequest.AppendEntriesResponse> handler) {

    }


    @Test
    public void testHandleRequest() throws PacificaException {
        RpcRequestHandler rpcRequestHandler = newHandler();
        RpcRequest.AppendEntriesRequest request = createRequest();
        rpcRequestHandler.handleRequest(rpcContext, request);
        Mockito.verify(this.replicaService).handleAppendLogEntryRequest(Mockito.eq(request), Mockito.any());

    }
}
