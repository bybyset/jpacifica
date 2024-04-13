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

package com.trs.pacifica.snapshot.storage;

import com.trs.pacifica.fs.remote.RemoteFileDownloader;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.SnapshotDownloader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class DefaultSnapshotDownloader implements SnapshotDownloader {

    static final String REMOTE_SNAPSHOT_META_FILE_SUFFIX = ".remote";

    private final RemoteFileDownloader remoteFileDownloader;

    private final SnapshotWriter snapshotWriter;

    private DefaultSnapshotMeta remoteSnapshotMeta = null;

    private volatile boolean canceled = false;

    private final CountDownLatch cdl = new CountDownLatch(1);

    private volatile Throwable exception = null;
    protected DefaultSnapshotDownloader(PacificaClient pacificaClient, ReplicaId remoteId, long remoteReaderId, SnapshotWriter snapshotWriter) {
        this.snapshotWriter = snapshotWriter;
        remoteFileDownloader = new RemoteFileDownloader(pacificaClient, remoteId, remoteReaderId);
    }

    @Override
    public void start() {
        try {
            startDownload();
        } catch (Throwable e) {
            this.exception = e;
        } finally {
            cdl.countDown();
        }
    }


    // TODO Break point resume
    private void startDownload() throws IOException {
        //1. list files of the remote snapshot
        loadRemoteSnapshotMeta();
        //2. download
        Collection<String> remoteFiles = this.remoteSnapshotMeta.listFiles();
        for (String filename : remoteFiles) {
            ensureCanceled();
            File downloadFile = new File(getLocalDownloadFile(filename));
            if (downloadFile.exists()) {
                FileUtils.forceDelete(downloadFile);
            }
            this.remoteFileDownloader.downloadToFile(filename, downloadFile);
        }

    }

    String getLocalDownloadFile(String filename) {
        return this.snapshotWriter.getDirectory() + File.separator + filename;
    }

    private void loadRemoteSnapshotMeta() throws IOException {
        final String downloadDir = this.snapshotWriter.getDirectory();
        final File downloadFile = new File(getRemoteSnapshotMetaFilePath(downloadDir));
        if (downloadFile.exists()) {
            FileUtils.forceDelete(downloadFile);
        }
        this.remoteFileDownloader.downloadToFile(DefaultSnapshotMeta.SNAPSHOT_META_FILE, downloadFile);
        this.remoteSnapshotMeta = DefaultSnapshotMeta.loadFromFile(downloadFile);
    }

    private void ensureCanceled() {
        if (canceled) {
            throw new CancellationException(this.getClass().getSimpleName() + " has canceled.");
        }
    }

    @Override
    public boolean cancel() {
        if (!canceled && this.cdl.getCount() > 0) {
            try {
                ensureCanceled();
            } catch (Throwable e) {
                this.exception = e;
            } finally {
                this.cdl.countDown();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean isCancelled() {
        return canceled;
    }

    @Override
    public void awaitComplete() throws InterruptedException, ExecutionException {
        cdl.await();
        if (this.exception != null) {
            throw new ExecutionException("", this.exception);
        }
    }

    @Override
    public boolean isCompleted() {
        return this.cdl.getCount()  <= 0;
    }

    @Override
    public void close() throws IOException {
        if (snapshotWriter != null) {
            snapshotWriter.close();
        }
    }

    static String getRemoteSnapshotMetaFilePath(final String dir) {
        return dir + File.separator + DefaultSnapshotMeta.SNAPSHOT_META_FILE + REMOTE_SNAPSHOT_META_FILE_SUFFIX;
    }


}
