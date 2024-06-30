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

import com.trs.pacifica.SnapshotStorage;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotDownloader;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.OnlyForTest;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultSnapshotStorage implements SnapshotStorage {
    static final Logger LOGGER = LoggerFactory.getLogger(DefaultSnapshotStorage.class);
    static final String SNAPSHOT_DIRECTORY_PREFIX = "snapshot_";

    static final String SNAPSHOT_WRITER_DIR = "_temp";

    private final String storagePath;

    /**
     * snapshot path name <-> ref count. eg: snapshot_1001 : 1
     */
    private final Map<String, AtomicInteger> refCounts = new ConcurrentHashMap<>();

    private long lastSnapshotIndex = 0;

    public DefaultSnapshotStorage(String storagePath) {
        this.storagePath = storagePath;
    }

    /**
     * @throws IOException
     */
    public void load() throws IOException {
        File storageDir = new File(storagePath);
        FileUtils.forceMkdir(storageDir);
        //
        File[] files = storageDir.listFiles();
        List<Long> snapshotLogIndexList = new ArrayList<>();
        for (File snapshotDir : files) {
            if (!snapshotDir.isDirectory()) {
                continue;
            }
            String snapshotDirName = snapshotDir.getName();
            if (!snapshotDirName.startsWith(SNAPSHOT_DIRECTORY_PREFIX)) {
                continue;
            }
            long snapshotLogIndex = Long.valueOf(snapshotDirName.substring(SNAPSHOT_DIRECTORY_PREFIX.length()));
            snapshotLogIndexList.add(snapshotLogIndex);
        }
        if (!snapshotLogIndexList.isEmpty()) {
            Collections.sort(snapshotLogIndexList);
            final int snapshotCount = snapshotLogIndexList.size();
            for (int i = 0; i < snapshotCount - 1; i++) {
                final long index = snapshotLogIndexList.get(i);
                final String snapshotPath = getSnapshotPath(index);
                destroySnapshot(snapshotPath);
            }
            this.lastSnapshotIndex = snapshotLogIndexList.get(snapshotCount - 1);
            incRef(getSnapshotName(this.lastSnapshotIndex));
        }
    }

    @OnlyForTest
    long getLastSnapshotIndex() {
        return this.lastSnapshotIndex;
    }

    String getSnapshotPath(final long logIndex) {
        return getSnapshotPath(getSnapshotName(logIndex));
    }

    String getSnapshotPath(final String snapshotName) {
        return this.storagePath + File.separator + snapshotName;
    }

    String getSnapshotName(final long logIndex) {
        return SNAPSHOT_DIRECTORY_PREFIX + logIndex;
    }

    /**
     * @param pathname the dir of will delete
     * @return true if success to delete dir
     * @throws IOException
     */
    boolean destroySnapshot(final String pathname) throws IOException {
        if (decRef(pathname)) {
            String path = getSnapshotPath(pathname);
            LOGGER.info("Deleting snapshot {}.", path);
            final File file = new File(path);
            FileUtils.deleteDirectory(file);
            return true;
        }
        return false;
    }


    void incRef(final String snapshotName) {
        final AtomicInteger ref = this.refCounts.computeIfAbsent(snapshotName, (name) -> {
            return new AtomicInteger(0);
        });
        ref.incrementAndGet();
    }

    /**
     * @param snapshotName
     * @return true if the file should be deleted
     */
    boolean decRef(final String snapshotName) {
        final AtomicInteger ref = this.refCounts.get(snapshotName);
        if (ref != null) {
            if (ref.decrementAndGet() <= 0) {
                this.refCounts.remove(snapshotName);
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    synchronized void setLastSnapshotIndex(final long lastSnapshotIndex) {
        final long oldLastSnapshotIndex = this.lastSnapshotIndex;
        if (oldLastSnapshotIndex < lastSnapshotIndex) {
            //set new last snapshot index
            this.lastSnapshotIndex = lastSnapshotIndex;
            incRef(getSnapshotName(lastSnapshotIndex));
            // delete old snapshot
            try {
                destroySnapshot(getSnapshotName(oldLastSnapshotIndex));
            } catch (IOException e) {
                LOGGER.warn("failed to destroy old snapshot {} ", getSnapshotName(oldLastSnapshotIndex), e);
            }
        }
    }

    protected String getTempSnapshotName() {
        return SNAPSHOT_WRITER_DIR;
    }

    int getRef(final String snapshotName) {
        final AtomicInteger ref = this.refCounts.get(snapshotName);
        if (ref != null) {
            return ref.get();
        } else {
            return 0;
        }
    }

    @Override
    public SnapshotReader openSnapshotReader() {
        final String snapshotName = getSnapshotName(this.lastSnapshotIndex);
        try {
            final DefaultSnapshotReader snapshotReader = new DefaultSnapshotReader(this, snapshotName);
            incRef(snapshotName);
            return snapshotReader;
        } catch (IOException e) {
            LOGGER.error("Failed to open snapshot reader, path={}", this.getSnapshotPath(snapshotName), e);
        }
        return null;
    }

    @Override
    public SnapshotWriter openSnapshotWriter(final LogId snapshotLogId) {
        final String tempSnapshotName = getTempSnapshotName();
        final String writerTempPath = getSnapshotPath(tempSnapshotName);
        try {
            do {
                final File tempFile = new File(writerTempPath);
                if (tempFile.exists()) {
                    if (getRef(tempSnapshotName) != 0) {
                        break;
                    }
                    if (!destroySnapshot(tempSnapshotName)) {
                        break;
                    }
                }
                FileUtils.forceMkdir(tempFile);
                incRef(tempSnapshotName);
                return new DefaultSnapshotWriter(snapshotLogId, this, tempSnapshotName);
            } while (false);
        } catch (IOException e) {
            LOGGER.error(String.format("Failed to open SnapshotWriter for path=%s.", writerTempPath), e);
        }
        return null;
    }


    @Override
    public SnapshotDownloader startDownloadSnapshot(DownloadContext downloadContext) {
        final LogId downloadLogId = downloadContext.getDownloadLogId();
        //SnapshotWriter
        final SnapshotWriter snapshotWriter = this.openSnapshotWriter(downloadLogId);
        if (snapshotWriter == null) {
            throw new RuntimeException("failed to open SnapshotWriter on start download snapshot.");
        }
        final DefaultSnapshotDownloader snapshotDownloader = new DefaultSnapshotDownloader(downloadContext.getPacificaClient(),
                downloadContext.getRemoteId(), downloadContext.getReaderId(), snapshotWriter, downloadContext.getTimeoutMs(), downloadContext.getDownloadExecutor());
        return snapshotDownloader;
    }


}
