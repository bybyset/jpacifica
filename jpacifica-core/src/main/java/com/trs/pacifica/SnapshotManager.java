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
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.proto.RpcRequest;

public interface SnapshotManager {


    /**
     * install snapshot
     * When Candidate receives a InstallSnapshotRequest from the Primary,
     * the Candidate downloads the snapshot file from the Primary
     * and loads it into its own state machine.
     *
     * @param installSnapshotRequest install snapshot request from Primary
     * @param callback               for async
     * @return InstallSnapshotResponse
     */
    RpcRequest.InstallSnapshotResponse installSnapshot(final RpcRequest.InstallSnapshotRequest installSnapshotRequest, final InstallSnapshotCallback callback);


    /**
     * execute snapshot
     */
    default void doSnapshot() {
        doSnapshot(null);
    }

    /**
     * execute snapshot
     *
     * @param callback called after finished.
     */
    void doSnapshot(final Callback callback);


    /**
     * get the LogId at last execute snapshot
     *
     * @return LogId
     */
    LogId getLastSnapshotLodId();


    abstract class InstallSnapshotCallback implements Callback {

        private RpcRequest.InstallSnapshotResponse response = null;

        public RpcRequest.InstallSnapshotResponse getResponse() {
            return response;
        }

        public void setResponse(RpcRequest.InstallSnapshotResponse response) {
            this.response = response;
        }
    }

}
