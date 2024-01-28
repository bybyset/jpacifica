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

package com.trs.pacifica.sender;

import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.util.TimeUtils;

public class SenderImpl implements Sender{

    private final ReplicaId fromId;

    private final ReplicaId toId;

    private SenderType type;

    private long lastResponseTime = Long.MIN_VALUE;


    public SenderImpl(ReplicaId fromId, ReplicaId toId, SenderType type) {
        this.fromId = fromId;
        this.toId = toId;
        this.type = type;
    }

    private void updateLastResponseTime() {
        this.lastResponseTime = TimeUtils.monotonicMs();
    }

    @Override
    public boolean isAlive() {
        return false;
    }
}
