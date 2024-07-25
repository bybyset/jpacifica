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

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.ReplicaId;

import java.util.concurrent.TimeUnit;

/**
 * Only called by Primary.
 */
public interface SenderGroup {

    default void addSenderTo(ReplicaId replicaId) throws PacificaException {
        addSenderTo(replicaId, SenderType.Secondary, false);
    }

    default void addSenderTo(ReplicaId replicaId, SenderType senderType) throws PacificaException {
        addSenderTo(replicaId, senderType, false);
    }


    /**
     * add Sender
     *
     * @param replicaId       replicaId
     * @param senderType      to see {@link SenderType}
     * @param checkConnection check connect
     * @throws PacificaException if error
     */
    void addSenderTo(ReplicaId replicaId, SenderType senderType, boolean checkConnection) throws PacificaException;


    /**
     * Whether the specified Replica is alive
     *
     * @param replicaId replicaId
     * @return true if the Replica is alive
     */
    boolean isAlive(ReplicaId replicaId);


    default boolean waitCaughtUp(final ReplicaId replicaId, final Sender.OnCaughtUp onCaughtUp, long timeout, TimeUnit unit) {
        return waitCaughtUp(replicaId, onCaughtUp, unit.toMillis(timeout));
    }

    /**
     * submit CaughtUp task and wait
     *
     * @param replicaId  replicaId
     * @param onCaughtUp callback
     * @param timeoutMs  timeout
     * @return true if success to submit CaughtUp task
     */
    boolean waitCaughtUp(final ReplicaId replicaId, final Sender.OnCaughtUp onCaughtUp, final long timeoutMs);


    /**
     * @param logIndex logIndex
     * @return true if continue
     */
    boolean continueAppendLogEntry(final long logIndex);


    /**
     * remove sender
     *
     * @param replicaId replicaId
     * @return sender of removed
     */
    Sender removeSender(ReplicaId replicaId);


}
