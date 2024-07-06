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
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class LinkedQueueSingleThreadExecutorGroup implements ExecutorGroup {


    static final int STATE_STARTED = 0;
    static final int STATE_SHUTDOWN = 1;
    static final int STATE_TERMINATED = 2;

    private static final AtomicIntegerFieldUpdater<LinkedQueueSingleThreadExecutorGroup> _STATE_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(LinkedQueueSingleThreadExecutorGroup.class, "state");
    private final Executor executor;
    private volatile int state = STATE_STARTED;

    private final RejectedExecutionHandler rejectedExecutionHandler;

    private final BlockingQueue<SingleThreadExecutor> singleThreadExecutors = new LinkedBlockingQueue<>();


    public LinkedQueueSingleThreadExecutorGroup(Executor executor) {
        this(executor, RejectedExecutionHandler.REJECT);
    }

    public LinkedQueueSingleThreadExecutorGroup(Executor executor, RejectedExecutionHandler rejectedExecutionHandler) {
        this.executor = executor;
        this.rejectedExecutionHandler = rejectedExecutionHandler;
    }


    @Override
    public boolean shutdownGracefully() {
        _STATE_UPDATER.compareAndSet(this, STATE_STARTED, STATE_SHUTDOWN);
        SingleThreadExecutor singleThreadExecutor = null;
        boolean success = true;
        while ((singleThreadExecutor = singleThreadExecutors.poll()) != null) {
            success &= singleThreadExecutor.shutdownGracefully();
        }
        _STATE_UPDATER.compareAndSet(this, STATE_SHUTDOWN, STATE_TERMINATED);
        return success;
    }

    @Override
    public boolean shutdownGracefully(long timeout, TimeUnit unit) {
        _STATE_UPDATER.compareAndSet(this, STATE_STARTED, STATE_SHUTDOWN);
        SingleThreadExecutor singleThreadExecutor = null;
        boolean success = true;
        while ((singleThreadExecutor = singleThreadExecutors.poll()) != null) {
            success &= singleThreadExecutor.shutdownGracefully(timeout, unit);
        }
        _STATE_UPDATER.compareAndSet(this, STATE_SHUTDOWN, STATE_TERMINATED);
        return success;
    }

    @Override
    public SingleThreadExecutor chooseExecutor() {
        if (!isShutdown()) {
            LinkedQueueSingleThreadExecutor singleThreadExecutor = new LinkedQueueSingleThreadExecutor(this.executor, rejectedExecutionHandler);
            SingleThreadExecutorFilter singleThreadExecutorFilter = new SingleThreadExecutorFilter(singleThreadExecutor) {
                @Override
                protected void onShutdown() {
                    LinkedQueueSingleThreadExecutorGroup.this.singleThreadExecutors.remove(this);
                }
            };
            singleThreadExecutors.offer(singleThreadExecutorFilter);
            return singleThreadExecutorFilter;
        }
        return null;
    }

    @Override
    public Iterator<SingleThreadExecutor> iterator() {
        return singleThreadExecutors.iterator();
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command");
        if (isShutdown()) {
            reject(command);
        }
        executor.execute(command);
    }

    protected final void reject(final Runnable task) {
        this.rejectedExecutionHandler.rejected(task, null);
    }


    public boolean isShutdown() {
        return this.state >= STATE_SHUTDOWN;
    }

    public boolean isTerminated() {
        return this.state == STATE_TERMINATED;
    }
}
