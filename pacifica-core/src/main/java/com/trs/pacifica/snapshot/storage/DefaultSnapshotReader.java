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

import com.trs.pacifica.snapshot.SnapshotMeta;
import com.trs.pacifica.snapshot.SnapshotReader;

import java.util.Collection;

public class DefaultSnapshotReader implements SnapshotReader {

    private final String snapshotName;

    private final DefaultSnapshotStorage snapshotStorage;

    public DefaultSnapshotReader(DefaultSnapshotStorage snapshotStorage, String snapshotName) {
        this.snapshotName = snapshotName;
        this.snapshotStorage = snapshotStorage;
    }

    @Override
    public SnapshotMeta getSnapshotMeta() {
        return null;
    }

    @Override
    public String getDirectory() {
        return this.snapshotStorage.getSnapshotPath(this.snapshotName);
    }

    @Override
    public Collection<String> listFiles() {
        return null;
    }

    @Override
    public long generateReadIdForDownload() {
        return 0;
    }
}
