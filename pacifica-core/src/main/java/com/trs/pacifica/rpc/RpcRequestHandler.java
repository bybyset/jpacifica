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
import com.trs.pacifica.async.DirectExecutor;
import com.trs.pacifica.error.PacificaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public abstract class RpcRequestHandler<Req extends Message, Rep extends Message> implements RpcHandler<Req, Rep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcRequestHandler.class);
    @Override
    public void handleRequest(RpcContext<Rep> rpcContext, Req request) {
        try {
            final RpcResponseCallback<Rep> callback = new SendOnceRpcResponseCallback(rpcContext);
            final Rep response = asyncHandleRequest(request, callback);
            if (response != null) {
                rpcContext.sendResponse(response);
            }
        } catch (PacificaException e) {
            //TODO

        }
    }

    protected abstract Rep asyncHandleRequest(final Req request, final RpcResponseCallback<Rep> rpcResponseCallback) throws PacificaException;

}
