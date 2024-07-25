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

package com.trs.pacifica;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * Given a pending index,
 * the queue is always consumed in the order of increasing pending index.
 * No element is consumed, pending index is incremented.
 *
 * @param <E> Element
 */
public interface PendingQueue<E> extends Iterable<E> {

    /**
     * get current pending index (log index)
     *
     * @return pending index
     */
    long getPendingIndex();

    /**
     * rest pending index (log index) and return a collection of abandoned elements
     *
     * @param pendIndex pendIndex
     * @return collection of abandoned elements
     */
    Collection<E> reset(final long pendIndex);


    /**
     * clear all element of the queue
     *
     * @return collection of abandoned elements
     */
    Collection<E> clear();

    /**
     * add element to queue
     *
     * @param e element it is nullable
     * @return true if success
     */
    boolean add(@Nullable E e);

    /**
     * Pops the element at the head of the queue,
     * and the pending index moves forward
     *
     * @return element it is nullable.
     */
    @Nullable
    E poll();

    /**
     * Pop the head of the queue element if and only if index is equal to pending index
     *
     * @param index log index
     * @return Element
     */
    E poll(final long index);

    List<E> pollUntil(final long endIndex);

    @Nullable
    E peek();

    int size();

    boolean isEmpty();

}
