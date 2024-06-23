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

package com.trs.pacifica.core;

import com.trs.pacifica.PendingQueue;

import javax.annotation.Nullable;
import java.util.*;

public class PendingQueueImpl<E> implements PendingQueue<E> {

    private final Queue<NodeContext<E>> queue;

    private long pendingIndex = 1;

    public PendingQueueImpl() {
        this.queue = new LinkedList<>();
    }

    @Override
    public long getPendingIndex() {
        return this.pendingIndex;
    }

    @Override
    public void resetPendingIndex(long pendIndex) {
        this.pendingIndex = pendIndex;
    }

    @Override
    public boolean add(@Nullable E e) {
        return queue.add(new NodeContext<>(e));
    }

    @Nullable
    @Override
    public E poll() {
        final NodeContext<E> nodeContext = queue.poll();
        if (nodeContext != null) {
            advancePendingIndex();
            return nodeContext.e;
        }
        return null;
    }

    @Override
    public List<E> pollUntil(final long endIndex) {
        List<E> list = new ArrayList<>();
        while (!this.isEmpty() && this.pendingIndex <= endIndex) {
            list.add(poll());
        }
        return list;
    }

    @Nullable
    @Override
    public E peek() {
        final NodeContext<E> nodeContext = this.queue.peek();
        if (nodeContext != null) {
            return nodeContext.e;
        }
        return null;
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    private void advancePendingIndex(){
        this.pendingIndex++;
    }

    @Override
    public Iterator<E> iterator() {
        Iterator<NodeContext<E>> iterator = PendingQueueImpl.this.queue.iterator();
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public E next() {
                final NodeContext<E> nodeContext = iterator.next();
                if (nodeContext != null) {
                    return nodeContext.e;
                }
                return null;
            }
        };
    }

    static final class NodeContext<E> {
        private final E e;

        NodeContext(@Nullable E e) {
            this.e = e;
        }
    }
}
