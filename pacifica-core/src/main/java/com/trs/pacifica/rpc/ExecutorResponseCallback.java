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

package com.trs.pacifica.rpc;

import com.google.protobuf.Message;
import com.trs.pacifica.async.DirectExecutor;
import com.trs.pacifica.async.Finished;

import java.util.concurrent.Executor;

public abstract class ExecutorResponseCallback <R extends Message> extends RpcResponseCallbackAdapter<R> {
    static final Executor DEFAULT_EXECUTOR = new DirectExecutor();
    private final Executor executor;


    protected ExecutorResponseCallback() {
        this(DEFAULT_EXECUTOR);
    }

    protected ExecutorResponseCallback(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void run(final Finished finished) {
        this.executor.execute(() -> {doRun(finished);});
    }

    protected abstract void doRun(Finished finished);
}
