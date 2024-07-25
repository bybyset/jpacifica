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

/**
 * For manage ReplicaService, to see {@link ReplicaService}
 */
public interface ReplicaServiceManager {

    /**
     * register ReplicaService
     *
     * @param replicaId replicaId
     * @param replicaService replicaService
     */
    void registerReplicaService(final ReplicaId replicaId, final ReplicaService replicaService);


    /**
     * Get the ReplicaService for the specified replicaId
     * Rpc request between replicas is directed to the
     * specified implementation of ReplicaService by its replicaId
     *
     * @param replicaId replicaId
     * @return null if not found
     */
    ReplicaService getReplicaService(final ReplicaId replicaId);


    /**
     * unregister ReplicaService
     *
     * @param replicaId replicaId
     * @return null if not found
     */
    ReplicaService unregisterReplicaService(final ReplicaId replicaId);


}
