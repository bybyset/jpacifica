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

import com.google.protobuf.Message;
import com.trs.pacifica.Replica;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.rpc.RpcContext;
import com.trs.pacifica.rpc.RpcRequestHandler;
import com.trs.pacifica.rpc.service.DefaultReplicaServiceManager;
import com.trs.pacifica.rpc.service.ReplicaService;
import com.trs.pacifica.rpc.service.ReplicaServiceManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public abstract class BaseReplicaRequestHandlerTest <Req extends Message, Rep extends Message>{

    private ReplicaService replica;
    private RpcContext<Rep> rpcContext;
    protected ReplicaServiceManager replicaServiceManager = new DefaultReplicaServiceManager();

    @BeforeEach
    public void setup() {
        this.replica = Mockito.mock(ReplicaService.class);
        this.rpcContext = createRpcContext();
    }

    @Test
    public void testHandleRequest() {
        final RpcRequestHandler<Req, Rep> handler = newHandler();
        handler.handleRequest(rpcContext, createRequest());
        verify(handler.interest(), replica, handler);
    }


    protected abstract RpcRequestHandler<Req, Rep> newHandler();
    protected abstract Req createRequest();
    protected abstract RpcContext<Rep> createRpcContext();
    public abstract void verify(String interest, ReplicaService service, RpcRequestHandler<Req, Rep> handler);





}
