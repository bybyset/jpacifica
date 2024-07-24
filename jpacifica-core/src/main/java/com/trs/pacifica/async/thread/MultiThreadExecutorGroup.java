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

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class MultiThreadExecutorGroup implements ExecutorGroup {

    static final ExecutorChooserFactory DEFAULT_EXECUTOR_CHOOSER_FACTORY = new DefaultExecutorChooserFactory();

    protected final SingleThreadExecutor[] children;

    protected final ExecutorChooser executorChooser;

    protected final Set<SingleThreadExecutor> readOnlyChildren;


    protected MultiThreadExecutorGroup(SingleThreadExecutor[] children, ExecutorChooserFactory executorChooserFactory) {
        this.children = Objects.requireNonNull(children, "children");
        int childrenNum = 0;
        for (; childrenNum < children.length; childrenNum++) {
            if (children[childrenNum] == null) {
                throw new IllegalArgumentException(String.format("the child thread is null at index=%d", childrenNum));
            }
        }
        if (childrenNum < 1) {
            throw new IllegalArgumentException("The number of child thread is at least 1");
        }
        if (executorChooserFactory == null) {
            this.executorChooser = DEFAULT_EXECUTOR_CHOOSER_FACTORY.newChooser(this.children);
        } else {
            this.executorChooser = executorChooserFactory.newChooser(this.children);
        }
        this.readOnlyChildren = toUnmodifiableSet(this.children);
    }

    @Override
    public SingleThreadExecutor chooseExecutor() {
        return this.executorChooser.chooseExecutor();
    }

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
