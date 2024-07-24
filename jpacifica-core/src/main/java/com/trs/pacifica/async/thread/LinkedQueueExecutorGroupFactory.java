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

import com.trs.pacifica.util.NamedThreadFactory;
import com.trs.pacifica.util.SystemConstants;
import com.trs.pacifica.util.SystemPropertyUtil;

import java.util.concurrent.*;

public class LinkedQueueExecutorGroupFactory implements ExecutorGroupFactory {

    static final int DEFAULT_THREADS_NUM = SystemPropertyUtil.getInt("pacifica.executor.group.thread.num", Math.max(16, SystemConstants.CPUS + 1));
    static final String DEFAULT_THREAD_NAME = "default-pacifica-single-thread-";

    static final Executor GLOABL_EXECUTOR;

    static {
        final int nThreads = DEFAULT_THREADS_NUM;
        final ThreadFactory threadFactory = new NamedThreadFactory(DEFAULT_THREAD_NAME, true);
        GLOABL_EXECUTOR = new ThreadPoolExecutor(nThreads,
                nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                threadFactory
        );

    }


    @Override
    public ExecutorGroup newExecutorGroup() {
        return new LinkedQueueSingleThreadExecutorGroup(GLOABL_EXECUTOR);
    }
}
