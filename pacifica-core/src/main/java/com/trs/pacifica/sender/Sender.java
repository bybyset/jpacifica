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
 * <li>Maintain the heartbeat of the Primary to other replica</li>
 * <li>Primary copies op log to other replica </li>
 *
 */
public interface Sender {


    /**
     * Check if the peer-to-peer heartbeat is alive
     * @param leasePeriodTimeOutMs 
     * @return true if alive
     */
    boolean isAlive(final int leasePeriodTimeOutMs);


    /**
     * get SenderType
     * @return
     */
    SenderType getType();


    boolean continueSendLogEntries(final long endLogIndex);


    boolean waitCaughtUp(OnCaughtUp onCaughtUp, final long timeoutMs);


    /**
     *
     * @throws PacificaException
     */
    void startup() throws PacificaException;

    /**
     *
     * @throws PacificaException
     */
    void shutdown() throws PacificaException;




    static abstract class OnCaughtUp implements Callback {

        private long caughtUpLogIndex = -1L;

        public void setCaughtUpLogIndex(long logIndex) {
            this.caughtUpLogIndex = logIndex;
        }

        public long getCaughtUpLogIndex() {
            return caughtUpLogIndex;
        }
    }


}
