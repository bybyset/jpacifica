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

import com.trs.pacifica.async.thread.chooser.ExecutorChooserFactory;
import com.trs.pacifica.util.NamedThreadFactory;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;

public class MpscSingleThreadExecutorGroup extends MultiThreadExecutorGroup {

    public static final String _DEFAULT_THREAD_NAME = "default-pacifica-single-thread-";

    public static final int _DEFAULT_MAX_PENDING_TASKS_PER_THREAD = Integer.MAX_VALUE;
    private final ThreadFactory threadFactory;

    private final int maxPendingTasksPerThread;

    private final RejectedExecutionHandler rejectedExecutionHandler;

    public MpscSingleThreadExecutorGroup(int nThreads) {
        this(nThreads, _DEFAULT_MAX_PENDING_TASKS_PER_THREAD, null, null, null);
    }

    public MpscSingleThreadExecutorGroup(int nThreads, ExecutorChooserFactory executorChooserFactory) {
        this(nThreads, _DEFAULT_MAX_PENDING_TASKS_PER_THREAD, executorChooserFactory, null, null);
    }

    public MpscSingleThreadExecutorGroup(int nThreads, int maxPendingTasksPerThread, ExecutorChooserFactory executorChooserFactory) {
        this(nThreads, maxPendingTasksPerThread, executorChooserFactory, null, null);
    }

    public MpscSingleThreadExecutorGroup(int nThreads, int maxPendingTasksPerThread, ExecutorChooserFactory executorChooserFactory, ThreadFactory threadFactory, RejectedExecutionHandler rejectedExecutionHandler) {
        super(nThreads, executorChooserFactory);
        if (maxPendingTasksPerThread <= 0) {
            throw new IllegalArgumentException("maxPendingTasksPerThread must be greater than 0.");
        }
        this.maxPendingTasksPerThread = maxPendingTasksPerThread;
        if (threadFactory == null) {
            this.threadFactory = new NamedThreadFactory(_DEFAULT_THREAD_NAME);
        } else {
            this.threadFactory = threadFactory;
        }
        if (rejectedExecutionHandler == null) {
            this.rejectedExecutionHandler = RejectedExecutionHandler.REJECT;
        } else {
            this.rejectedExecutionHandler = rejectedExecutionHandler;
        }
    }

    @Override
    protected SingleThreadExecutor newChild() throws Exception {
        return new MpscSingleThreadExecutor(maxPendingTasksPerThread, threadFactory, rejectedExecutionHandler);
    }


}
