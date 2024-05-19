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

import com.google.protobuf.Message;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.rpc.node.Endpoint;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public interface ReplicaClient {

    /**
     * connect to targetReplicaId
     *
     * @param targetReplicaId
     * @return true if success
     */
    default boolean connect(ReplicaId targetReplicaId) {
        return checkConnection(targetReplicaId, true);
    }


    /**
     * Check the connection for the given targetReplicaId,
     * and if there is no connection, create a new address.
     *
     * @param targetReplicaId
     * @param createIfAbsent
     * @return true if keep connected
     */
    boolean checkConnection(ReplicaId targetReplicaId, boolean createIfAbsent);


    /**
     *
     * @param endpoint
     * @param request
     * @param callback
     * @param timeoutMs
     * @param callbackExecutor
     * @return
     * @param <T>
     */
    public <T extends Message> Future<Message> sendRequest(final Endpoint endpoint, final Message request, final RpcRequestFinished<T> callback, final int timeoutMs, final Executor callbackExecutor);

}
