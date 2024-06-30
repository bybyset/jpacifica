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

package com.trs.pacifica.fs.remote;

import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import org.mockito.Mockito;

public class RemoteFileDownloaderTest {


    private RemoteFileDownloader remoteFileDownloader;

    private PacificaClient pacificaClient;

    private ReplicaId remoteId = new ReplicaId("test_group", "test_node");

    private long readId = 1L;


    public void setup() {
        this.pacificaClient = Mockito.mock(PacificaClient.class);
        this.remoteFileDownloader = new RemoteFileDownloader(this.pacificaClient, this.remoteId, this.readId);
    }

}
