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

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LinkedQueueSingleThreadExecutor implements SingleThreadExecutor {

    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 15 * 1000;


    static final int STATE_STARTED = 0;
    static final int STATE_SHUTDOWN = 1;
    static final int STATE_TERMINATED = 2;

    private final Executor executor;
    private volatile int state = STATE_STARTED;
    private final RejectedExecutionHandler rejectedExecutionHandler;
    private Task headTask = null;
    private Task tailTask = null;

    public LinkedQueueSingleThreadExecutor(Executor executor) {
        this(executor, RejectedExecutionHandler.REJECT);
    }

    public LinkedQueueSingleThreadExecutor(Executor executor, RejectedExecutionHandler rejectedExecutionHandler) {
        this.executor = executor;
        this.rejectedExecutionHandler = rejectedExecutionHandler;
    }

    @Override
    public boolean shutdownGracefully() {
        return shutdownGracefully(DEFAULT_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean shutdownGracefully(long timeout, TimeUnit unit) {
        //TODO impl

        return false;
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command");
        if (isShutdown()) {
            reject(command);
        }
        addTask(command);
    }

    private synchronized void addTask(final Runnable command) {
        final Task currentTask = new Task(command);
        // set prev
        currentTask.setPrev(this.tailTask);
        if (this.tailTask != null) {
            // set next
            this.tailTask.setNext(currentTask);
            this.tailTask = currentTask;
        } else {
            this.tailTask = currentTask;
            doExecute(currentTask);
        }
    }

    private void doExecute(Task task) {
        if (task != null) {
            this.headTask = task;
            this.executor.execute(task);
        }
    }

    private synchronized void onTaskFinish(Task finishedTask) {
        if (finishedTask != null) {
            final Task next = finishedTask.getNext();
            doExecute(next);
            //help gc
            finishedTask.setNext(null);
            finishedTask.setPrev(null);
            if (this.tailTask == finishedTask) {
                this.tailTask = null;
            }
        }
    }

    public boolean isShutdown() {
        return this.state >= STATE_SHUTDOWN;
    }

    public boolean isTerminated() {
        return this.state == STATE_TERMINATED;
    }

    protected final void reject(final Runnable task) {
        this.rejectedExecutionHandler.rejected(task, this);
    }

    class Task implements Runnable {
        private final Runnable run;
        private Task prev = null;
        private Task next = null;

        Task(Runnable run) {
            this.run = run;
        }

        void setNext(Task next) {
            this.next = next;
        }

        void setPrev(Task prev) {
            this.prev = prev;
        }

        public Task getPrev() {
            return prev;
        }

        public Task getNext() {
            return next;
        }

        @Override
        public void run() {
            try {
                this.run.run();
            } finally {
                // do next
                onTaskFinish(this);
            }
        }

    }
}
