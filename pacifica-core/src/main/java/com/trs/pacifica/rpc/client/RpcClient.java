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

package com.trs.pacifica.rpc.client;

import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.rpc.node.Endpoint;

public interface RpcClient extends LifeCycle<RpcClient.Option> {


    /**
     * Check connection for given address.
     *
     * @param endpoint target address
     * @return true if there is a connection and the connection is active and writable.
     */
    default boolean checkConnection(final Endpoint endpoint) {
        return checkConnection(endpoint, false);
    }


    /**
     * Check connection for given address and async to create a new one if there is no connection.
     *
     * @param endpoint       target address
     * @param createIfAbsent create a new one if there is no connection
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent);


    /**
     * Asynchronous invocation with a callback.
     *
     * @param endpoint  target address
     * @param request   request object
     * @param ctx       invoke context
     * @param callback  invoke callback
     * @param timeoutMs timeout millisecond
     */
    void invokeAsync(final Endpoint endpoint, final Object request, final Object ctx, final Callback callback,
                     final long timeoutMs);



    public static class Option {

        public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 8 * 1024 * 1024;

        private int maxInboundMessageSize = DEFAULT_MAX_INBOUND_MESSAGE_SIZE;

        public int getMaxInboundMessageSize() {
            return maxInboundMessageSize;
        }

        public void setMaxInboundMessageSize(int maxInboundMessageSize) {
            this.maxInboundMessageSize = maxInboundMessageSize;
        }
    }


}
