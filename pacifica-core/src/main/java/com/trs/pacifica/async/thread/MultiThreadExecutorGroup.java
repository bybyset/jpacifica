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

import com.trs.pacifica.async.thread.chooser.DefaultExecutorChooserFactory;
import com.trs.pacifica.async.thread.chooser.ExecutorChooser;
import com.trs.pacifica.async.thread.chooser.ExecutorChooserFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class MultiThreadExecutorGroup implements ExecutorGroup {

    protected final SingleThreadExecutor[] children;

    protected final ExecutorChooser executorChooser;

    protected final Set<SingleThreadExecutor> readOnlyChildren;


    protected MultiThreadExecutorGroup(int nThreads, ExecutorChooserFactory executorChooserFactory) {
        if (nThreads <= 0) {
            throw new IllegalArgumentException(String.format("nThreads: %d (expected: > 0)", nThreads));
        }
        this.children = new SingleThreadExecutor[nThreads];
        for (int i = 0; i < nThreads; i ++) {
            boolean success = false;
            try {
                children[i] = newChild();
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
        if (executorChooserFactory == null) {
            this.executorChooser = new DefaultExecutorChooserFactory().newChooser(this.children);
        } else {
            this.executorChooser = executorChooserFactory.newChooser(this.children);
        }
        this.readOnlyChildren = toUnmodifiableSet(this.children);

    }

    @Override
    public SingleThreadExecutor chooseExecutor() {
        return this.executorChooser.chooseExecutor();
    }

    /**
     * Create a new SingleThreadExecutor which will later then accessible via the {@link #chooseExecutor()}  method. This method will be
     * called for each thread that will serve this {@link MultiThreadExecutorGroup}.
     */
    protected abstract SingleThreadExecutor newChild() throws Exception;

    @Override
    public boolean shutdownGracefully() {
        boolean success = true;
        for (final SingleThreadExecutor c : this.children) {
            if (c != null) {
                success = success && c.shutdownGracefully();
            }
        }
        return success;
    }

    @Override
    public boolean shutdownGracefully(long timeout, TimeUnit unit) {
        boolean success = true;
        final long timeoutNanos = unit.toNanos(timeout);
        final long start = System.nanoTime();
        for (final SingleThreadExecutor c : this.children) {
            if (c == null) {
                continue;
            }
            success = success && c.shutdownGracefully(timeout, unit);
            if (System.nanoTime() - start > timeoutNanos) {
                success = false;
                break;
            }
        }
        return success;
    }

    @Override
    public Iterator<SingleThreadExecutor> iterator() {
        return this.readOnlyChildren.iterator();
    }

    @Override
    public void execute(Runnable command) {
        chooseExecutor().execute(command);
    }

    private static Set<SingleThreadExecutor> toUnmodifiableSet(final SingleThreadExecutor[] children) {
        final Set<SingleThreadExecutor> tmp = new LinkedHashSet<>();
        Collections.addAll(tmp, children);
        return Collections.unmodifiableSet(tmp);
    }
}
