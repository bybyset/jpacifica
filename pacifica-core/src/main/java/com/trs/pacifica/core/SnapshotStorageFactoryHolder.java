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

import com.trs.pacifica.SnapshotStorageFactory;
import com.trs.pacifica.spi.JPacificaServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotStorageFactoryHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogStorageFactoryHolder.class);

    private static final SnapshotStorageFactory SNAPSHOT_STORAGE_FACTORY;

    static {
        SNAPSHOT_STORAGE_FACTORY = JPacificaServiceLoader//
                .load(SnapshotStorageFactory.class)//
                .first();
        LOGGER.info("use SnapshotStorageFactory={}", SNAPSHOT_STORAGE_FACTORY.getClass().getName());
    }


    public static final SnapshotStorageFactory getInstance() {
        return SNAPSHOT_STORAGE_FACTORY;
    }

    private SnapshotStorageFactoryHolder() {

    }
}
