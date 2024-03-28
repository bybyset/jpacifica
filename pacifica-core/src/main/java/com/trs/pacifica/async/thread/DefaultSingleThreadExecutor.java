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

import com.trs.pacifica.util.ExecutorServiceHelper;
import com.trs.pacifica.util.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultSingleThreadExecutor implements SingleThreadExecutor {
    private final SingleThreadExecutor singleThreadExecutor;

    /**
     * Anti-gentleman is not against villains, we believe that you are
     * providing a single-thread executor.
     *
     * @param singleThreadExecutorService a {@link ExecutorService} instance
     */
    public DefaultSingleThreadExecutor(ExecutorService singleThreadExecutorService) {
        this.singleThreadExecutor = wrapSingleThreadExecutor(singleThreadExecutorService);
    }

    public DefaultSingleThreadExecutor(String poolName, int maxPendingTasks) {
        this.singleThreadExecutor = createSingleThreadExecutor(poolName, maxPendingTasks);
    }

    @Override
    public void execute(final Runnable task) {
        this.singleThreadExecutor.execute(task);
    }

    @Override
    public boolean shutdownGracefully() {
        return this.singleThreadExecutor.shutdownGracefully();
    }

    @Override
    public boolean shutdownGracefully(final long timeout, final TimeUnit unit) {
        return this.singleThreadExecutor.shutdownGracefully(timeout, unit);
    }

    private static SingleThreadExecutor wrapSingleThreadExecutor(final ExecutorService executor) {
        if (executor instanceof SingleThreadExecutor) {
            return (SingleThreadExecutor) executor;
        } else {
            return new SingleThreadExecutor() {

                @Override
                public boolean shutdownGracefully() {
                    return ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
                }

                @Override
                public boolean shutdownGracefully(final long timeout, final TimeUnit unit) {
                    return ExecutorServiceHelper.shutdownAndAwaitTermination(executor, unit.toMillis(timeout));
                }

                @Override
                public void execute(final Runnable command) {
                    executor.execute(command);
                }
            };
        }
    }

    private static SingleThreadExecutor createSingleThreadExecutor(final String poolName, final int maxPendingTasks) {
        final ExecutorService singleThreadPool =
                new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(maxPendingTasks), new NamedThreadFactory(poolName, true));
        return new SingleThreadExecutor() {

            @Override
            public boolean shutdownGracefully() {
                return ExecutorServiceHelper.shutdownAndAwaitTermination(singleThreadPool);
            }

            @Override
            public boolean shutdownGracefully(final long timeout, final TimeUnit unit) {
                return ExecutorServiceHelper.shutdownAndAwaitTermination(singleThreadPool, unit.toMillis(timeout));
            }

            @Override
            public void execute(final Runnable command) {
                singleThreadPool.execute(command);
            }
        };
    }
}