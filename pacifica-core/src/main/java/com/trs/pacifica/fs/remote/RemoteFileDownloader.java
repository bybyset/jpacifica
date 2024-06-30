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

import com.trs.pacifica.async.DirectExecutor;
import com.trs.pacifica.async.Task;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class RemoteFileDownloader {

    static final Logger LOGGER = LoggerFactory.getLogger(RemoteFileDownloader.class);

    private final PacificaClient pacificaClient;
    private final ReplicaId remoteId;
    private final long remoteReaderId;

    public RemoteFileDownloader(PacificaClient pacificaClient, ReplicaId remoteId, long remoteReaderId) {
        this.pacificaClient = pacificaClient;
        this.remoteId = remoteId;
        this.remoteReaderId = remoteReaderId;
    }

    public void downloadToOutputStream(final String filename, final OutputStream outputStream) throws IOException {
        Task task = asyncDownloadToOutputStream(filename, outputStream);
        try {
            task.awaitComplete();
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

    public Task asyncDownloadToOutputStream(final String filename, final OutputStream outputStream, int timeoutMs, Executor executor) throws IOException {
        Objects.requireNonNull(filename, "filename");
        Objects.requireNonNull(outputStream, "outputStream");
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be greater than 0");
        }
        if (executor == null) {
            executor = new DirectExecutor();
        }

        DownloadSession downloadSession = new DownloadSession(this.pacificaClient, this.remoteId, remoteReaderId, filename, timeoutMs, executor) {
            @Override
            protected void onDownload(byte[] bytes) throws IOException {
                outputStream.write(bytes);
            }

            @Override
            protected boolean onFinish() {
                try {
                    outputStream.flush();
                    return true;
                } catch (IOException e) {
                    LOGGER.error("Failed to flush on download file={}", filename, e);
                    return false;
                }
            }
        };
        return downloadSession;
    }

    public Task asyncDownloadToOutputStream(final String filename, final OutputStream outputStream) throws IOException {
        return asyncDownloadToOutputStream(filename, outputStream, DownloadSession.DEFAULT_TIMEOUT_MS, new DirectExecutor());
    }

    public void downloadToFile(final String filename, final File destFile) throws IOException, FileNotFoundException {
        Objects.requireNonNull(filename, "filename");
        Objects.requireNonNull(destFile, "file");
        try (final BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(destFile))) {
            downloadToOutputStream(filename, outputStream);
        }
    }

    public Task asyncDownloadToFile(final String filename, final File destFile, int timeout, Executor executor) throws IOException, FileNotFoundException {
        Objects.requireNonNull(filename, "filename");
        Objects.requireNonNull(destFile, "file");
        try (final BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(destFile))) {
            return asyncDownloadToOutputStream(filename, outputStream, timeout, executor);
        }
    }

}
