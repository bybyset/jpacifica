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

package com.trs.pacifica.util.thread;

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.ExecutorCallback;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.util.NamedThreadFactory;
import com.trs.pacifica.util.SystemConstants;
import com.trs.pacifica.util.SystemPropertyUtil;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadUtil {


    private ThreadUtil() {
    }

    /**
     * Default pacifica callback executor pool minimum size, CPUs by default.
     */
    public static final int MIN_CALLBACK_THREAD_NUM = SystemPropertyUtil.getInt("pacifica.callback.thread.num.min", SystemConstants.CPUS);

    /**
     * Default pacifica callback executor pool maximum size.
     */
    public static final int MAX_CALLBACK_THREAD_NUM = SystemPropertyUtil.getInt("pacifica.callback.thread.num.max", Math.max(32, SystemConstants.CPUS * 5));

    static final ThreadPoolExecutor _CALLBACK_EXECUTOR = ThreadPoolUtil.newBuilder()//
            .poolName("pacifica-callback-executor")//
            .enableMetric(true)//
            .coreThreads(MIN_CALLBACK_THREAD_NUM)//
            .maximumThreads(MAX_CALLBACK_THREAD_NUM)//
            .keepAliveSeconds(60L)//
            .workQueue(new LinkedBlockingQueue<>())
            .threadFactory(new NamedThreadFactory("pacifica-callback-thread-", true))
            .build();


    /**
     * run callback in executor
     * for {@link ExecutorCallback} will run in specified executor {@link  ExecutorCallback#executor()}
     * @param callback
     * @param result
     */
    public static void runCallback(final Callback callback, final Finished result) {
        if (callback == null) {
            return;
        }
        Executor executor = null;
        if (callback instanceof ExecutorCallback) {
            executor = ((ExecutorCallback) callback).executor();
        }
        if (executor == null) {
            executor = _CALLBACK_EXECUTOR;
        }
        executor.execute(() -> {
            callback.run(result);
        });
    }

}
