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
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.google.protobuf.Message;
import com.trs.pacifica.example.counter.MetaReplicaRpc;
import com.trs.pacifica.example.counter.config.jraft.MetaReplicaClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class RpcMetaReplicaClosure<R> extends MetaReplicaClosure<R> {

    static final Logger LOGGER = LoggerFactory.getLogger(RpcMetaReplicaClosure.class);
    private static final AtomicIntegerFieldUpdater<RpcMetaReplicaClosure> STATE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(RpcMetaReplicaClosure.class, "state");

    private static final int PENDING = 0;
    private static final int RESPOND = 1;
    private final RpcContext rpcCtx;

    private volatile int state = PENDING;

    public RpcMetaReplicaClosure(RpcContext rpcCtx) {
        this.rpcCtx = rpcCtx;
    }

    @Override
    public void run(Status status) {
        Message response = null;
        if (status.isOk()) {
            response = buildRpcResponse(getResult());
        } else {
            response = MetaReplicaRpc.ErrorResponse.newBuilder()//
                    .setCode(status.getCode())//
                    .setMsg(status.getErrorMsg())//
                    .build();
        }
        sendResponse(response);
    }

    public abstract Message buildRpcResponse(R result);


    public void sendResponse(Message msg) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, RESPOND)) {
            LOGGER.warn(String.format("A response: %s sent repeatedly!", msg));
            return;
        }
        this.rpcCtx.sendResponse(msg);
    }

}
