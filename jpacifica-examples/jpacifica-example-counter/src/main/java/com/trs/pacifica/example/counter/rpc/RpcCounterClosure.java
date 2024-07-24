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
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.example.counter.replica.CounterClosure;
import com.trs.pacifica.rpc.RpcContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class RpcCounterClosure<R, Res extends Message> extends CounterClosure<R> {

    static final Logger LOGGER = LoggerFactory.getLogger(RpcCounterClosure.class);
    private static final AtomicIntegerFieldUpdater<RpcCounterClosure> STATE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(RpcCounterClosure.class, "state");

    private static final int PENDING = 0;
    private static final int RESPOND = 1;
    private final RpcContext<Res> rpcCtx;

    private volatile int state = PENDING;

    public RpcCounterClosure(RpcContext<Res> rpcCtx) {
        this.rpcCtx = rpcCtx;
    }

    @Override
    public void run(Finished finished) {
        Res response = null;
        if (finished.isOk()) {
            response = buildRpcResponse(getResult());
        } else {
        }
        sendResponse(response);
    }

    public abstract Res buildRpcResponse(R result);

    public void sendResponse(Res msg) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, RESPOND)) {
            LOGGER.warn(String.format("A response: %s sent repeatedly!", msg));
            return;
        }
        this.rpcCtx.sendResponse(msg);
    }

}
