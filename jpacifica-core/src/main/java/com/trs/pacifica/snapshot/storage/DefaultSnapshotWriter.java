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

import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.IOUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultSnapshotWriter extends SnapshotWriter {

    private final DefaultSnapshotMeta snapshotMeta;
    private final DefaultSnapshotStorage snapshotStorage;
    private final String pathName;

    private boolean closed = false;

    public DefaultSnapshotWriter(final LogId snapshotLogId, DefaultSnapshotStorage snapshotStorage, String pathName) {
        super(snapshotLogId);
        this.snapshotStorage = snapshotStorage;
        this.pathName = pathName;
        this.snapshotMeta = DefaultSnapshotMeta.newSnapshotMeta(snapshotLogId);
    }

    @Override
    public String getDirectory() {
        return this.snapshotStorage.getSnapshotPath(this.pathName);
    }

    @Override
    public Collection<String> listFiles() {
        return this.snapshotMeta.listFiles();
    }

    @Override
    public boolean addFile(String filename) {
        ensureClosed();
        return this.snapshotMeta.addFile(filename, null);
    }

    @Override
    public boolean removeFile(String filename) {
        ensureClosed();
        return this.snapshotMeta.removeFile(filename);
    }

    @Override
    public synchronized void close() throws IOException {
        //TODO check successful
        if (!closed) {
            try {
                // save snapshot log id
                saveSnapshotMeta();
                // rename temp dir to snapshot dir
                atomicMove();
                // set last snapshot index
                this.snapshotStorage.setLastSnapshotIndex(this.snapshotLogId.getIndex());
            } finally {
                this.snapshotStorage.destroySnapshot(this.pathName);
            }
            this.closed = true;
        }
    }

    void saveSnapshotMeta() throws IOException {
        String snapshotMetaFilePath = DefaultSnapshotMeta.getSnapshotMetaFilePath(this.getDirectory());
        DefaultSnapshotMeta.saveToFile(this.snapshotMeta, snapshotMetaFilePath, true);
    }



    void atomicMove() throws IOException {
        final String sourceDirPath = getDirectory();
        final long snapshotLogIndex = this.snapshotLogId.getIndex();
        final String targetDirPath = this.snapshotStorage.getSnapshotPath(snapshotLogIndex);
        File sourceDir = new File(sourceDirPath);
        File targetDir = new File(targetDirPath);
        FileUtils.forceDeleteOnExit(targetDir);
        IOUtils.atomicMoveFile(sourceDir, targetDir, true);
    }


    void ensureClosed() {
        if (closed) {
            throw new AlreadyClosedException(this.getClass().getSimpleName() + " already closed.");
        }
    }
}
