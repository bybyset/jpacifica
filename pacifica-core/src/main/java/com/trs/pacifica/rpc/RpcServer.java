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
import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.rpc.client.RpcClient;

public interface RpcServer extends LifeCycle<RpcServer.Option> {


    void registerRpcHandler(final RpcHandler<?, ?> rpcHandler);


    public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 16 * 1024 * 1024;

    public static final int DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE = 16 * 1024 * 1024;

    public static class Option {

        private int maxInboundMessageSize = DEFAULT_MAX_INBOUND_MESSAGE_SIZE;

        private int maxOutboundMessageSize = DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE;

        public int getMaxInboundMessageSize() {
            return maxInboundMessageSize;
        }

        public void setMaxInboundMessageSize(int maxInboundMessageSize) {
            this.maxInboundMessageSize = maxInboundMessageSize;
        }

        public int getMaxOutboundMessageSize() {
            return maxOutboundMessageSize;
        }

        public void setMaxOutboundMessageSize(int maxOutboundMessageSize) {
            this.maxOutboundMessageSize = maxOutboundMessageSize;
        }
    }


}
