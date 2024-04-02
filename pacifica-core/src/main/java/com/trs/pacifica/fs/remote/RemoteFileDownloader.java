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

import java.io.*;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class RemoteFileDownloader {

    private final PacificaClient pacificaClient;

    private final ReplicaId remoteId;

    private final long remoteReaderId;


    public RemoteFileDownloader(PacificaClient pacificaClient, ReplicaId remoteId, long remoteReaderId) {
        this.pacificaClient = pacificaClient;
        this.remoteId = remoteId;
        this.remoteReaderId = remoteReaderId;
    }


    public void downloadToOutputStream(final String filename, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(filename, "filename");
        Objects.requireNonNull(outputStream, "outputStream");
        DownloadSession downloadSession = new DownloadSession(this.pacificaClient, this.remoteId, remoteReaderId, filename) {
            @Override
            protected void onDownload(byte[] bytes) throws IOException {
                outputStream.write(bytes);
            }
        };
        try {
            downloadSession.awaitComplete();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException(cause);
            }
        }
    }

    public void downloadToFile(final String filename, final File destFile) throws IOException, FileNotFoundException {
        Objects.requireNonNull(filename, "filename");
        Objects.requireNonNull(destFile, "file");
        try (final BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(destFile))) {
            downloadToOutputStream(filename, outputStream);
            outputStream.flush();
        }
    }


}
