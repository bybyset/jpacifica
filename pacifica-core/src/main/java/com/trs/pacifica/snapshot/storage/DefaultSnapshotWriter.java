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

import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultSnapshotWriter extends SnapshotWriter {

    private final Map<String, Object> fileContainer = new ConcurrentHashMap<>();

    private final DefaultSnapshotStorage snapshotStorage;
    private final String pathName;


    public DefaultSnapshotWriter(final LogId snapshotLogId, DefaultSnapshotStorage snapshotStorage, String pathName) {
        super(snapshotLogId);
        this.snapshotStorage = snapshotStorage;
        this.pathName = pathName;
    }



    @Override
    public String getDirectory() {
        return this.snapshotStorage.getSnapshotPath(this.pathName);
    }

    @Override
    public Collection<String> listFiles() {
        return null;
    }

    @Override
    public boolean addFile(String filename) {
        return this.fileContainer.putIfAbsent(filename, null) == null;
    }

    @Override
    public boolean removeFile(String filename) {
        this.fileContainer.remove(filename);
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            // save snapshot log id

            // rename temp dir to snapshot dir


        } finally {
            this.snapshotStorage.decRef(this.pathName);
        }
    }

    void saveSnapshotMeta() {

    }

    void atomicMove() throws IOException {
        final String sourceDirPath = getDirectory();
        File sourceDir = new File(sourceDirPath);
        File targetDir = new File("");
        IOUtils.atomicMoveDirectory(sourceDir, targetDir, true);

    }

}
