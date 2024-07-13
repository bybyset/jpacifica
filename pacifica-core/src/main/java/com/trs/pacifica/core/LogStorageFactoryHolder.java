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

import com.trs.pacifica.LogStorageFactory;
import com.trs.pacifica.spi.JPacificaServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStorageFactoryHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogStorageFactoryHolder.class);

    private static final LogStorageFactory LOG_STORAGE_FACTORY;

    static {
        LOG_STORAGE_FACTORY = JPacificaServiceLoader//
                .load(LogStorageFactory.class)//
                .first();
        LOGGER.info("use LogStorageFactory={}", LOG_STORAGE_FACTORY.getClass().getName());
    }


    public static final LogStorageFactory getInstance() {
        return LOG_STORAGE_FACTORY;
    }

    private LogStorageFactoryHolder() {

    }
}
