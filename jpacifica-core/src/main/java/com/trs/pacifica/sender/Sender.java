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

/**
 * implement:
 * Maintain the heartbeat of the Primary to other replica.
 * Primary copies op log to other replica.
 * Primary send install snapshot request to other replica.
 */
public interface Sender {


    /**
     * Check if the peer-to-peer heartbeat is alive
     *
     * @param leasePeriodTimeOutMs lease period  ms
     * @return true if alive
     */
    boolean isAlive(final int leasePeriodTimeOutMs);


    /**
     * get SenderType
     * to see {@link SenderType}
     *
     * @return SenderType
     */
    SenderType getType();


    /**
     * When the primary replica has new OP log stored persistently,
     * this function will be called to continue sending new OP log.
     *
     * @param endLogIndex op-logs are sent up to the end specified log index
     * @return true if continue send
     */
    boolean continueSendLogEntries(final long endLogIndex);


    /**
     * Primary receive recovery request from Candidate,
     * Callback after waiting for the OP log of the Candidate to catch up with the Primary,
     * until time out.
     *
     * @param onCaughtUp Callback when the OP log of the Candidate catches up with the Primary.
     * @param timeoutMs  timeout
     * @return true if success caught up
     */
    boolean waitCaughtUp(OnCaughtUp onCaughtUp, final long timeoutMs);


    /**
     * @throws PacificaException if error
     */
    void startup() throws PacificaException;

    /**
     * @throws PacificaException if error
     */
    void shutdown() throws PacificaException;


    static interface OnCaughtUp extends Callback {

        public void setCaughtUpLogIndex(long logIndex);

        public long getCaughtUpLogIndex();

        public long getGroupVersion();

        public void setGroupVersion(long groupVersion);
    }


}
