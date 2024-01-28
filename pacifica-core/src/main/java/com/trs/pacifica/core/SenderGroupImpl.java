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

package com.trs.pacifica.core;

import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.sender.Sender;
import com.trs.pacifica.sender.SenderGroup;
import com.trs.pacifica.sender.SenderType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SenderGroupImpl implements SenderGroup {


    private final PacificaClient pacificaClient;

    private final Map<ReplicaId, Sender> senderContainer = new ConcurrentHashMap<>();

    public SenderGroupImpl(PacificaClient pacificaClient) {
        this.pacificaClient = pacificaClient;
    }

    @Override
    public boolean addSenderTo(ReplicaId replicaId, SenderType senderType, boolean checkConnection) {
        return false;
    }

    @Override
    public boolean isAlive(ReplicaId replicaId) {
        return false;
    }

    @Override
    public Sender removeSender(ReplicaId replicaId) {
        return null;
    }

    @Override
    public void clear() {

    }

    


}
