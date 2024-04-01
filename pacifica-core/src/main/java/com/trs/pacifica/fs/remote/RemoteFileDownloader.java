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

import com.trs.pacifica.async.Finished;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.ExecutorResponseCallback;
import com.trs.pacifica.rpc.client.PacificaClient;

import java.io.OutputStream;
import java.util.concurrent.ExecutionException;

public class RemoteFileDownloader {


    private final PacificaClient pacificaClient;

    private final ReplicaId remoteId;





    public RemoteFileDownloader(PacificaClient pacificaClient, ReplicaId remoteId) {
        this.pacificaClient = pacificaClient;
        this.remoteId = remoteId;
    }


    public void downloadToOutputStream(final String filename, final OutputStream outputStream) {

    }

    public void downloadToFile(final String filename, final OutputStream outputStream) {

    }


    static final int DEFAULT_GET_FILE_REQUEST_TIMEOUT_MS = 5 * 60 * 1000;

    class DownloadSession {


        private final RpcRequest.GetFileRequest.Builder requestBuilder;

        private final int requestTimeoutMs = DEFAULT_GET_FILE_REQUEST_TIMEOUT_MS;


        DownloadSession(RpcRequest.GetFileRequest.Builder requestBuilder) {
            this.requestBuilder = requestBuilder;
        }

        void continueDownload() {

            RpcRequest.GetFileRequest request = requestBuilder
                    .setOffset(0)//
                    .setLength(0)//
                    .build();

            pacificaClient.getFile(request, new ExecutorResponseCallback<RpcRequest.GetFileResponse>() {
                @Override
                protected void doRun(Finished finished) {
                    if (finished.isOk()) {
                        handleGetFileResponse(getRpcResponse());
                    } else {
                        handleGetFileError(finished.error());
                    }
                }
            }, requestTimeoutMs);

        }

        void handleGetFileResponse(RpcRequest.GetFileResponse response) {
            //handle success

            //
            continueDownload();
        }

        void handleGetFileError(Throwable throwable) {

        }

        void awaitComplete() throws InterruptedException, ExecutionException {

        }


    }


}
