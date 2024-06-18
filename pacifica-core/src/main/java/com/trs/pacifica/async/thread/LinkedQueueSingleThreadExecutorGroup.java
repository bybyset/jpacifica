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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LinkedQueueSingleThreadExecutorGroup implements ExecutorGroup {


    private final Executor executor;

    private final RejectedExecutionHandler rejectedExecutionHandler;

    private final BlockingQueue<SingleThreadExecutor> singleThreadExecutors = new LinkedBlockingQueue<>();

    public LinkedQueueSingleThreadExecutorGroup(Executor executor, RejectedExecutionHandler rejectedExecutionHandler) {
        this.executor = executor;
        this.rejectedExecutionHandler = rejectedExecutionHandler;
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
    public SingleThreadExecutor chooseExecutor() {
        LinkedQueueSingleThreadExecutor singleThreadExecutor = new LinkedQueueSingleThreadExecutor(this.executor, rejectedExecutionHandler);
        singleThreadExecutors.offer(singleThreadExecutor);
        return singleThreadExecutor;
    }

    @Override
    public Iterator<SingleThreadExecutor> iterator() {
        return singleThreadExecutors.iterator();
    }

    @Override
    public void execute(Runnable command) {

    }
}
