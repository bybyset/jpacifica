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

package com.trs.pacifica.rpc;

import com.google.protobuf.Message;
import com.trs.pacifica.async.Finished;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class SendOnceRpcResponse<Rep extends Message> extends RpcRequestFinishedAdapter<Rep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendOnceRpcResponse.class);

    private static final AtomicIntegerFieldUpdater<SendOnceRpcResponse> STATE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(SendOnceRpcResponse.class, "state");

    private static final int PENDING = 0;
    private static final int RESPOND = 1;
    private volatile int state = PENDING;
    private final RpcContext<Rep> rpcContext;
    private final Rep defaultResponse;

    public SendOnceRpcResponse(RpcContext<Rep> rpcContext, Rep defaultResponse) {
        this.rpcContext = rpcContext;
        this.defaultResponse = defaultResponse;
    }


    @Override
    public void run(Finished finished) {
        Rep response = getRpcResponse();
        if (response == null) {
            response = defaultResponse;
        }
        sendResponse(response);
    }

    public void sendResponse(final Rep msg) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, RESPOND)) {
            LOGGER.warn("A response: {} sent repeatedly!", msg);
            return;
        }
        this.rpcContext.sendResponse(msg);
    }


}
