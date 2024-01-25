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

public interface ReConfiguration {


    /**
     * After the Candidate replica completes the recovery process,
     * the Primary replica sends an add_secondary request to the configuration cluster.
     * @param version current version of replica group
     * @param replicaId replicaId of add
     * @return ture if success
     */
    public boolean addSecondary(final long version, final ReplicaId replicaId);

    /**
     * After the Primary replica detects the failure of the Secondary replica,
     * the Primary replica sends the remove_secondary request to the configuration cluster.
     * remove Secondary
     * @param version current version of replica group
     * @param replicaId replicaId of removed
     * @return ture if success
     */
    public boolean removeSecondary(final long version, final ReplicaId replicaId);

    /**
     * After the Secondary replica detects the failure of the Primary replica,
     * the Secondary replica sends the change_primary request to the configuration cluster.
     * @param version
     * @param replicaId
     * @return
     */
    public boolean changePrimary(final long version, final ReplicaId replicaId);


}
