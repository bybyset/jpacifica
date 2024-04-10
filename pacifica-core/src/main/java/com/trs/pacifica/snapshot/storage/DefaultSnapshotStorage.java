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

    static final String SNAPSHOT_META_FILE = "_snapshot_meta";

    static final String SNAPSHOT_WRITER_DIR = "_temp";

    private final String storagePath;

    private final Map<String, AtomicInteger> refCounts = new ConcurrentHashMap<>();

    private long lastSnapshotIndex = 0;

    public DefaultSnapshotStorage(String storagePath) throws IOException {
        this.storagePath = storagePath;
        load();
    }

    private void load() throws IOException {
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
     * @param path the dir of will delete
     * @return true if success to delete dir
     * @throws IOException
     */
    private boolean destroySnapshot(final String path) throws IOException {
        if (decRef(path)) {
            LOGGER.info("Deleting snapshot {}.", path);
            final File file = new File(path);
            FileUtils.deleteDirectory(file);
            return true;
        }
        return false;
    }


    void incRef(final String pathname) {
        final AtomicInteger ref = this.refCounts.computeIfAbsent(pathname, (name) -> {
            return new AtomicInteger(0);
        });
        ref.incrementAndGet();
    }

    /**
     * @param pathname
     * @return true if the file should be deleted
     */
    boolean decRef(final String pathname) {
        final AtomicInteger ref = this.refCounts.get(pathname);
        if (ref != null) {
            if (ref.decrementAndGet() <= 0) {
                this.refCounts.remove(pathname);
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    protected String getTempSnapshotName() {
        return SNAPSHOT_WRITER_DIR;
    }

    int getRef(final String pathname) {
        final AtomicInteger ref = this.refCounts.get(pathname);
        if (ref != null) {
            return ref.get();
        } else {
            return 0;
        }
    }

    @Override
    public SnapshotReader openSnapshotReader() {
        final String snapshotName = getSnapshotName(this.lastSnapshotIndex);
        incRef(snapshotName);
        final DefaultSnapshotReader snapshotReader = new DefaultSnapshotReader(this, snapshotName);
        return snapshotReader;
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
                incRef(tempSnapshotName);
                FileUtils.forceMkdir(tempFile);
                return new DefaultSnapshotWriter(snapshotLogId, this, tempSnapshotName);
            } while (false);
        } catch (IOException e) {
            LOGGER.error("", e);
        }
        return null;
    }


    @Override
    public SnapshotDownloader startDownloadSnapshot(DownloadContext downloadContext) {
        return null;
    }


    void atomicMoveTemp() {

    }

}
