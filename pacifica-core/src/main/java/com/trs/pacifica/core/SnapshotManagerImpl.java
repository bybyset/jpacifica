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

package com.trs.pacifica.core;

import com.trs.pacifica.LifeCycle;
import com.trs.pacifica.SnapshotManager;
import com.trs.pacifica.SnapshotStorage;
import com.trs.pacifica.SnapshotStorageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class SnapshotManagerImpl implements SnapshotManager, LifeCycle<SnapshotManagerImpl.Option> {

    static final Logger LOGGER = LoggerFactory.getLogger(SnapshotManagerImpl.class);

    private Option option;


    private SnapshotStorage snapshotStorage;

    @Override
    public void init(Option option) {
        this.option = Objects.requireNonNull(option, "option");
        final String storagePath = Objects.requireNonNull(option.getStoragePath(), "storage path");
        final SnapshotStorageFactory snapshotStorageFactory = Objects.requireNonNull(option.getSnapshotStorageFactory(), "snapshot storage factory");
        this.snapshotStorage = snapshotStorageFactory.newSnapshotStorage(storagePath);

    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public SnapshotStorage getSnapshotStorage() {
        return snapshotStorage;
    }

    @Override
    public void doSnapshot() {

    }

    public static class Option {

        private String storagePath;

        private SnapshotStorageFactory snapshotStorageFactory;

        public String getStoragePath() {
            return storagePath;
        }

        public void setStoragePath(String storagePath) {
            this.storagePath = storagePath;
        }

        public SnapshotStorageFactory getSnapshotStorageFactory() {
            return snapshotStorageFactory;
        }

        public void setSnapshotStorageFactory(SnapshotStorageFactory snapshotStorageFactory) {
            this.snapshotStorageFactory = snapshotStorageFactory;
        }
    }

}
