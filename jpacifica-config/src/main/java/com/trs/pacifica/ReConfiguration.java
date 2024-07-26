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

package com.trs.pacifica;

import com.trs.pacifica.model.ReplicaId;

/**
 * conf cluster interface for manager replica group
 * A replica group consists of multiple replica and version number.
 * Replica group contains the one and only group name.
 * Replicas are distributed in different nodes, and
 * the group name and the node name form the ReplicaId.
 * <p>
 * About version of replica group:
 * It is a monotonically increasing integer value,
 * an is incremented every time the state of the replica group change.
 * When the version number in the request is less than the version number in the replica group,
 * we reject the request and return false.
 */
public interface ReConfiguration {


    /**
     * After the Candidate replica completes the recovery process,
     * the Primary replica sends an add_secondary request to the configuration cluster.
     * When the conf cluster accepts and successfully processes the request,
     * the replica is upgraded to Secondary by the Candidate
     *
     * @param version   current version of replica group
     * @param replicaId replicaId of add
     * @return ture if success
     */
    boolean addSecondary(final long version, final ReplicaId replicaId);

    /**
     * After the Primary replica detects the failure of the Secondary replica,
     * the Primary replica sends the remove_secondary request to the configuration cluster.
     * remove Secondary
     * <p>
     * When the conf cluster accepts and successfully processes the request,
     * the replica is demoted from Secondary to Candidate.
     *
     * @param version   current version of replica group
     * @param replicaId replicaId of removed
     * @return ture if success
     */
    boolean removeSecondary(final long version, final ReplicaId replicaId);

    /**
     * After the Secondary replica detects the failure of the Primary replica,
     * the Secondary replica sends the change_primary request to the configuration cluster.
     * <p>
     * When the conf cluster accepts and successfully processes the request,
     * the replica is demoted from Primary to Candidate.
     *
     * @param version version
     * @param replicaId replicaId of new primary
     * @return true if success
     */
    boolean changePrimary(final long version, final ReplicaId replicaId);


}
