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

package com.trs.pacifica.async;

import java.util.concurrent.ExecutionException;

public interface Task {

    /**
     * Attempts to cancel execution of this task.  This method has no
     * effect if the task is already completed or cancelled, or could
     * not be cancelled for some other reason.  Otherwise, if this
     * task has not started when {@code cancel} is called, this task
     * should never run.
     *
     * <p>The return value from this method does not necessarily
     * indicate whether the task is now cancelled; use {@link
     * #isCancelled}.
     *
     * @return {@code false} if the task could not be cancelled,
     * typically because it has already completed; {@code true}
     * otherwise. If two or more threads cause a task to be cancelled,
     * then at least one of them returns {@code true}. Implementations
     * may provide stronger guarantees.
     */
    boolean cancel();

    /**
     * Returns {@code true} if this task was cancelled before it completed
     * normally.
     *
     * @return {@code true} if this task was cancelled before it completed
     */
    boolean isCancelled();

    /**
     * @throws java.util.concurrent.CancellationException if the computation was cancelled
     * @throws ExecutionException                         if the computation threw an
     *                                                    exception
     * @throws InterruptedException                       if the current thread was interrupted
     *                                                    while waiting
     */
    void awaitComplete() throws InterruptedException, ExecutionException;

    /**
     * Returns {@code true} if this task completed.
     * <p>
     * Completion may be due to normal termination, an exception, or
     * cancellation -- in all of these cases, this method will return
     * {@code true}.
     *
     * @return {@code true} if this task completed
     */
    boolean isCompleted();


}
