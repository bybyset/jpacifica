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

import com.trs.pacifica.core.ReplicaOption;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.SnapshotDownloader;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;

import java.io.IOException;
import java.util.concurrent.Executor;

public interface SnapshotStorage {


    /**
     * open a SnapshotReader,
     * when the replica loading snapshot, will ask for a SnapshotReader to be opened.
     *
     * @return
     */
    SnapshotReader openSnapshotReader();

    /**
     * open a SnapshotWriter for snapshotLogId
     * when the replica saving snapshot, will ask for a SnapshotWriter to be opened.
     * snapshotLogId: Passed by PacificA, the user is expected to persist the store
     * so that it can be loaded and read in SnapshotReader
     *
     * @param snapshotLogId
     * @return
     */
    SnapshotWriter openSnapshotWriter(final LogId snapshotLogId);


    /**
     * Start the task of downloading snapshots.
     *
     * @param downloadContext
     * @return
     */
    SnapshotDownloader startDownloadSnapshot(final DownloadContext downloadContext);


    public static class DownloadContext {

        private final LogId downloadLogId;

        private final long readerId;

        private final PacificaClient pacificaClient;

        private final ReplicaId remoteId;

        private Executor downloadExecutor;

        private int timeoutMs = ReplicaOption.DEFAULT_DOWNLOAD_SNAPSHOT_TIMEOUT_MS;

        public DownloadContext(LogId downloadLogId, long readerId, PacificaClient pacificaClient, ReplicaId remoteId) {
            this.downloadLogId = downloadLogId;
            this.readerId = readerId;
            this.pacificaClient = pacificaClient;
            this.remoteId = remoteId;
        }

        public LogId getDownloadLogId() {
            return downloadLogId;
        }

        public long getReaderId() {
            return readerId;
        }

        public PacificaClient getPacificaClient() {
            return pacificaClient;
        }

        public ReplicaId getRemoteId() {
            return remoteId;
        }

        public Executor getDownloadExecutor() {
            return downloadExecutor;
        }
        public void setDownloadExecutor(Executor downloadExecutor) {
            this.downloadExecutor = downloadExecutor;
        }

        public int getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(int timeoutMs) {
            this.timeoutMs = timeoutMs;
        }
    }

}
