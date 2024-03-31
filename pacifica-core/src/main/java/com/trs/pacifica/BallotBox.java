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

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;

import javax.annotation.Nullable;
import java.util.concurrent.locks.Lock;

/**
 * ballot box, managed by primary replica.
 */
public interface BallotBox {


    /**
     * initiate ballot for the operation.
     * @param replicaGroup replica group
     * @return true if success
     */
    public boolean initiateBallot(final ReplicaGroup replicaGroup);

    /**
     * cancel ballot of the replicaId
     * @param replicaId
     * @return
     */
    public boolean cancelBallot(final ReplicaId replicaId);


    /**
     * recover ballot of the replicaId from log index
     * @param replicaId
     * @param startLogIndex
     * @return
     */
    public boolean recoverBallot(final ReplicaId replicaId, final long startLogIndex);


    /**
     * called by primary.
     * receive the ballots of replicaId, [startLogIndex, endLogIndex]
     * When the quorum is satisfied, we commit
     * @param replicaId
     * @param startLogIndex
     * @param endLogIndex
     * @throws IllegalArgumentException if startLogIndex > endLogIndex
     * @return true is success
     */
    public boolean ballotBy(final ReplicaId replicaId, final long startLogIndex, final long endLogIndex);


    public Lock getCommitLock();

    /**
     * get last committed log index
     *
     * @return
     */
    public long getLastCommittedLogIndex();


}
