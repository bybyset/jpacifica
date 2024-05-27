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
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;

import java.util.concurrent.ExecutionException;

public interface StateMachineCaller {


    /**
     * Move the commit point forward,
     * and the log of user actions committed is applied to the state machine in order.
     *
     * @param logIndex
     * @return
     */
    boolean commitAt(final long logIndex);


    /**
     * get commit point (last log entry index to apply state machine)
     *
     * @return LogId(0, 0) if nothing commit,
     */
    LogId getCommitPoint();


    /**
     * get committing log index
     *
     * @return
     */
    long getCommittingLogIndex();

    /**
     * get committed log index
     *
     * @return
     */
    long getCommittedLogIndex();

    /**
     * Snapshot loading event.
     * Triggered when first started or after the Candidate
     * has downloaded a snapshot from the Primary.
     *
     * @param snapshotLoadCallback
     * @return
     */
    boolean onSnapshotLoad(final SnapshotLoadCallback snapshotLoadCallback);

    /**
     * snapshot saving event.
     * This event is fired by a user call to {@link Replica#snapshot(Callback)}
     * or by a timed schedule.
     *
     * @param snapshotSaveCallback
     * @return
     */
    boolean onSnapshotSave(final SnapshotSaveCallback snapshotSaveCallback);


    /**
     * Called when error happens.
     *
     * @param error PacificaException
     */
    void onError(final PacificaException error);


    static interface SnapshotLoadCallback extends Callback {

        public SnapshotReader getSnapshotReader();

        /**
         * wait snapshot load
         *
         * @throws InterruptedException if loading snapshot was interrupted.
         * @throws ExecutionException   if loading snapshot failed.
         */
        public void awaitComplete() throws InterruptedException, ExecutionException;


    }

    static interface SnapshotSaveCallback extends Callback {

        /**
         * @param saveLogId
         * @return
         */
        SnapshotWriter start(final LogId saveLogId);

        /**
         * get save LogId on snapshot save
         *
         * @return null if not start
         */
        LogId getSaveLogId();

    }

}
