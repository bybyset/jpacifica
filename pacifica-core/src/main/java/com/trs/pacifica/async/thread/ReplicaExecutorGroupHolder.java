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

package com.trs.pacifica.async.thread;

import com.trs.pacifica.spi.JPacificaServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaExecutorGroupHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaExecutorGroupHolder.class);

    private static final ExecutorGroupFactory _EXECUTOR_GROUP_FACTORY;
    private static ExecutorGroup INSTANCE;

    static {
        _EXECUTOR_GROUP_FACTORY = JPacificaServiceLoader//
                .load(ExecutorGroupFactory.class)//
                .first();
        LOGGER.info("use ExecutorGroupFactory={}", _EXECUTOR_GROUP_FACTORY.getClass().getName());
    }

    private ReplicaExecutorGroupHolder() {
    }

    public static ExecutorGroup getDefaultInstance() {
        if (INSTANCE == null) {
            synchronized (ReplicaExecutorGroupHolder.class) {
                if (INSTANCE == null) {
                    INSTANCE = _EXECUTOR_GROUP_FACTORY.newExecutorGroup();
                }
            }
        }
        return INSTANCE;
    }

}
