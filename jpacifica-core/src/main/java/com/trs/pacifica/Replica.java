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
import com.trs.pacifica.core.ReplicaState;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.Operation;
import com.trs.pacifica.model.ReplicaId;

public interface Replica {


    /**
     * get id of the Replica
     *
     * @return ReplicaId
     */
    ReplicaId getReplicaId();

    /**
     * get state of the Replica
     *
     * @param block true if thread safe is needed
     * @return state of Replica
     */
    ReplicaState getReplicaState(final boolean block);

    /**
     * get state of the Replica
     * default not thread-safe
     *
     * @return ReplicaState
     */
    default ReplicaState getReplicaState() {
        return getReplicaState(false);
    }


    /**
     * Whether the replica is the Primary.
     *
     * @param block true if thread safe is needed
     * @return ture if state of the replica is Primary
     */
    default boolean isPrimary(boolean block) {
        return ReplicaState.Primary == getReplicaState(block);
    }

    /**
     * Whether the replica is the Primary.
     * default not thread-safe
     * @see #isPrimary(boolean)
     *
     * @return true if is primary
     */
    default boolean isPrimary() {
        return isPrimary(false);
    }

    /**
     * called by user.
     * asynchronous apply operation to the replicated-state-machine,
     *
     * @param operation operation
     */
    void apply(Operation operation);


    /**
     * The snapshot is started immediately,
     * and the callback is executed when the snapshot is completed.
     *
     * @param onFinish callback
     */
    void snapshot(Callback onFinish);


    /**
     * Replica recovery is started immediately ,
     * and the callback is executed when the snapshot is completed.
     *
     * @param onFinish callback
     */
    void recover(Callback onFinish);


    /**
     * get LogId of the last committed
     *
     * @return LogId(0, 0) if nothing
     */
    LogId getCommitPoint();


    /**
     * get LogId of the last snapshot
     *
     * @return LogId(0, 0) if nothing
     */
    LogId getSnapshotLogId();


    /**
     * get LogId at first
     *
     * @return LogId(0, 0) if nothing
     */
    public LogId getFirstLogId();


    /**
     * get LogId at last
     *
     * @return LogId(0, 0) if nothing
     */
    public LogId getLastLogId();


}
