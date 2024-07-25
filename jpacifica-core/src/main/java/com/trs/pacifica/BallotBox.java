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

import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;

import java.util.concurrent.locks.Lock;

/**
 * <p>
 * ballot box, managed by primary replica.
 * After receiving all votes from the Secondary replicas,
 * move forward the commit point, which is the callback {@link StateMachineCaller#commitAt(long)}
 * </p>
 * <p>
 * Quorum:<br>
 * {@code N < W + R } <br>
 * {@code W = N} <br>
 * {@code R = 1} <br>
 * </p>
 */
public interface BallotBox {


    /**
     * initiate ballot for the operation.
     *
     * @param logIndex     the log index at initiate ballot
     * @param replicaGroup replica group
     * @return true if success
     */
    boolean initiateBallot(final long logIndex, final ReplicaGroup replicaGroup);

    /**
     * cancel ballot of the replicaId
     *
     * @param replicaId replicaId
     * @return true if success
     */
    boolean cancelBallot(final ReplicaId replicaId);


    /**
     * recover ballot of the replicaId from log index
     *
     * @param replicaId     replicaId
     * @param startLogIndex startLogIndex
     * @return true if success
     */
    boolean recoverBallot(final ReplicaId replicaId, final long startLogIndex);


    /**
     * called by primary.
     * receive the ballots of replicaId, [startLogIndex, endLogIndex]
     * When the quorum is satisfied, we commit
     *
     * @param replicaId     replicaId
     * @param startLogIndex startLogIndex
     * @param endLogIndex   endLogIndex
     * @return true is success
     * @throws IllegalArgumentException if startLogIndex greater than endLogIndex
     */
    boolean ballotBy(final ReplicaId replicaId, final long startLogIndex, final long endLogIndex);


    /**
     * @return commitLock
     */
    Lock getCommitLock();

    /**
     * get last committed log index
     *
     * @return log index
     */
    long getLastCommittedLogIndex();


}
