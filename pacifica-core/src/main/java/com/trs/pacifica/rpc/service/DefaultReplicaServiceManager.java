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

package com.trs.pacifica.rpc.service;

import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.spi.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@SPI
public class DefaultReplicaServiceManager implements ReplicaServiceManager{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReplicaServiceManager.class);

    private final Map<ReplicaId, ReplicaService> container = new ConcurrentHashMap<>();

    @Override
    public void registerReplicaService(ReplicaId replicaId, ReplicaService replicaService) {
        Objects.requireNonNull(replicaId, "replicaId");
        Objects.requireNonNull(replicaService, "replicaService");
        this.container.put(replicaId, replicaService);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("register ReplicaService for replicaId={}.", replicaId);
        }
    }

    @Override
    public ReplicaService getReplicaService(ReplicaId replicaId) {
        Objects.requireNonNull(replicaId, "replicaId");
        return this.container.get(replicaId);
    }

    @Override
    public ReplicaService unregisterReplicaService(ReplicaId replicaId) {
        Objects.requireNonNull(replicaId, "replicaId");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("unregister ReplicaService for replicaId={}.", replicaId);
        }
        return this.container.remove(replicaId);
    }


}
