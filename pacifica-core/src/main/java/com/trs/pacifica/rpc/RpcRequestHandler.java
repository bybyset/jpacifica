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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.util.RpcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RpcRequestHandler<Req extends Message, Rep extends Message> implements RpcHandler<Req, Rep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcRequestHandler.class);

    private final Rep defaultMessage;

    protected RpcRequestHandler(Rep defaultMessage) {
        this.defaultMessage = defaultMessage;
    }

    @Override
    public void handleRequest(RpcContext<Rep> rpcContext, Req request) {
        final SendOnceRpcResponse<Rep> callback = new SendOnceRpcResponse(rpcContext, defaultMessage);
        try {
            final Rep response = asyncHandleRequest(request, callback);
            if (response != null) {
                callback.sendResponse(response);
            }
        } catch (PacificaException e) {
            final Rep response = toErrorResponse(e);
            callback.sendResponse(response);
        } catch (Throwable throwable) {
            LOGGER.warn("Unexpected Exception.", throwable);
            final Rep response = toErrorResponse(new PacificaException(PacificaErrorCode.UNDEFINED, throwable.getMessage()));
            callback.sendResponse(response);
        }
    }

    protected abstract Rep asyncHandleRequest(final Req request, final RpcRequestFinished<Rep> rpcResponseCallback) throws PacificaException;


    /**
     * When an exception is encountered while handle a request,
     * we will catch that exception and return it to the client.
     *
     * @param exception
     * @return
     */
    protected Rep toErrorResponse(PacificaException exception) {
        final RpcRequest.ErrorResponse errorResponse = RpcUtil.toErrorResponse(exception);
        final Descriptors.FieldDescriptor errorFd = RpcUtil.findErrorFieldDescriptor(defaultMessage);
        return (Rep) defaultMessage.toBuilder()//
                .setField(errorFd, errorResponse)//
                .build();
    }


}
