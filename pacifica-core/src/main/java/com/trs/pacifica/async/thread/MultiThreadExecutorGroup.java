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

import java.util.Iterator;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class MultiThreadExecutorGroup implements ExecutorGroup {

    private final SingleThreadExecutor[] children;

    private final ExecutorChooser executorChooser;


    protected MultiThreadExecutorGroup(int nThreads, ExecutorChooserFactory executorChooserFactory, Object... args) {
        if (nThreads <= 0) {
            throw new IllegalArgumentException(String.format("nThreads: %d (expected: > 0)", nThreads));
        }
        this.children = new SingleThreadExecutor[nThreads];
        for (int i = 0; i < nThreads; i ++) {
            boolean success = false;
            try {
                children[i] = newChild(args);
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

        this.executorChooser = executorChooserFactory.newChooser(this.children);

    }

    /**
     * Create a new EventExecutor which will later then accessible via the {@link #next()}  method. This method will be
     * called for each thread that will serve this {@link MultiThreadExecutorGroup}.
     */
    protected abstract SingleThreadExecutor newChild(Object... args) throws Exception;

    @Override
    public SingleThreadExecutor next() {
        return null;
    }

    @Override
    public boolean shutdownGracefully() {
        return false;
    }

    @Override
    public boolean shutdownGracefully(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public Iterator<SingleThreadExecutor> iterator() {
        return null;
    }

    @Override
    public void execute(Runnable command) {

    }
}
