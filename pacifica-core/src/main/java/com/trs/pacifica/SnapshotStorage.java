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

import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.SnapshotDownloader;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;

public interface SnapshotStorage {


    public SnapshotReader openSnapshotReader();

    public SnapshotWriter openSnapshotWriter();


    /**
     * @return
     */
    public SnapshotDownloader startDownloadSnapshot(final DownloadContext downloadContext);


    public static class DownloadContext {

        private final long readerId;

        private final PacificaClient pacificaClient;

        private final ReplicaId remoteId;

        public DownloadContext(long readerId, PacificaClient pacificaClient, ReplicaId remoteId) {
            this.readerId = readerId;
            this.pacificaClient = pacificaClient;
            this.remoteId = remoteId;
        }
    }

}
