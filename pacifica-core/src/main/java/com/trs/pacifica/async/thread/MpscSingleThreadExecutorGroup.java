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

import com.trs.pacifica.async.thread.chooser.ExecutorChooser;
import com.trs.pacifica.async.thread.chooser.ExecutorChooserFactory;
import com.trs.pacifica.util.NamedThreadFactory;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

public class MpscSingleThreadExecutorGroup extends MultiThreadExecutorGroup {

    public static final String _DEFAULT_THREAD_NAME = "default-pacifica-single-thread-";

    public static final int _DEFAULT_MAX_PENDING_TASKS_PER_THREAD = Integer.MAX_VALUE;

    public MpscSingleThreadExecutorGroup(SingleThreadExecutor[] children, ExecutorChooserFactory executorChooserFactory) {
        super(children, executorChooserFactory);
    }

    public MpscSingleThreadExecutorGroup(SingleThreadExecutor[] children) {
        super(children, null);
    }

    private static MpscSingleThreadExecutor newChild(int maxPendingTasksPerThread, ThreadFactory threadFactory, RejectedExecutionHandler rejectedExecutionHandler) {
        return new MpscSingleThreadExecutor(maxPendingTasksPerThread, threadFactory, rejectedExecutionHandler);
    }

    static MpscSingleThreadExecutorGroup newThreadExecutorGroup(int nThreads) {
        return newThreadExecutorGroup(nThreads, null, _DEFAULT_MAX_PENDING_TASKS_PER_THREAD, null, null);
    }

    static MpscSingleThreadExecutorGroup newThreadExecutorGroup(int nThreads, ThreadFactory threadFactory, int maxPendingTasksPerThread,
                                                  RejectedExecutionHandler rejectedExecutionHandler, ExecutorChooserFactory executorChooserFactory) {
        if (nThreads <= 0) {
            throw new IllegalStateException("The number of threads is at least 1");
        }
        if (threadFactory == null) {
            threadFactory = new NamedThreadFactory(_DEFAULT_THREAD_NAME);
        }
        if (rejectedExecutionHandler == null) {
            rejectedExecutionHandler = RejectedExecutionHandler.REJECT;
        }
        if (maxPendingTasksPerThread <= 0) {
            maxPendingTasksPerThread = _DEFAULT_MAX_PENDING_TASKS_PER_THREAD;
        }

        SingleThreadExecutor[] children = new SingleThreadExecutor[nThreads];
        for (int i = 0; i < nThreads; i ++) {
            boolean success = false;
            try {
                children[i] = newChild(maxPendingTasksPerThread, threadFactory, rejectedExecutionHandler);
                success = true;
            } catch (Exception e) {
                throw new IllegalStateException("failed to create a child SingleThreadExecutor", e);
            } finally {
                if (!success) {
                    for (int j = 0; j < i; j ++) {
                        children[j].shutdownGracefully();
                    }
                }
            }
        }
        return new MpscSingleThreadExecutorGroup(children, executorChooserFactory);
    }

}
