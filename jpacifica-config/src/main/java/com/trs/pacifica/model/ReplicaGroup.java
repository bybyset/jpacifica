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

package com.trs.pacifica.model;

import com.trs.pacifica.ReConfiguration;
import com.trs.pacifica.model.ReplicaId;

import java.util.List;

/**
 * group of replica:
 * <p>
 * 1、consisting of a single primary replica and multiple secondary replica. <br>
 * 2、containing a self-incrementing version number in order to implement a "write request" to replica group <br>
 *    the "write request" containing "add secondary request","remove secondary request","change primary request"<br>
 * 3、In order to simplify the complexity of implementation, the concept of "primary term" is designed. <br>
 *    about "primary term": It will increment when the primary replica changes
 * </p>
 */
public interface ReplicaGroup {


    /**
     * get name of the replica group
     *
     * @return group name
     */
    public String getGroupName();

    /**
     * get ID of the Primary replica
     *
     * @return ReplicaId of Primary
     */
    public ReplicaId getPrimary();

    /**
     * list ID of the Secondary replica
     *
     * @return secondaries
     */
    public List<ReplicaId> listSecondary();

    /**
     * get current version,
     * When we send these requests to the "configuration cluster", we carry the version.
     * requests is them:
     *
     * @return version
     * @see ReConfiguration#addSecondary(long, ReplicaId)
     * @see ReConfiguration#removeSecondary(long, ReplicaId)
     * @see ReConfiguration#changePrimary(long, ReplicaId)
     */
    public long getVersion();


    /**
     * get term of the current primary replica
     *
     * @return primary term
     */
    public long getPrimaryTerm();


}
