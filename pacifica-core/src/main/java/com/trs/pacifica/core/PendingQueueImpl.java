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
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class PendingQueueImpl<E> implements PendingQueue<E> {

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    private final Queue<NodeContext<E>> queue;

    private long pendingIndex = 1;

    public PendingQueueImpl() {
        this.queue = new LinkedList<>();
    }

    @Override
    public long getPendingIndex() {
        this.readLock.lock();
        try {
            return this.pendingIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public Collection<E> reset(long pendIndex) {
        this.writeLock.lock();
        try{
            long oldPendingIndex = this.pendingIndex;
            this.pendingIndex = pendIndex;
            if (oldPendingIndex < pendIndex) {
                List<E> abandonedElements = new ArrayList<>();
                while (!this.queue.isEmpty() && oldPendingIndex < pendIndex) {
                    NodeContext<E> nodeContext = this.queue.poll();
                    if (nodeContext != null && nodeContext.e != null) {
                        abandonedElements.add(nodeContext.e);
                    }
                    oldPendingIndex++;
                }
                return abandonedElements;
            } else {
                return Collections.emptyList();
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Collection<E> clear() {
        this.writeLock.lock();
        try {
            List<E> elements = new ArrayList<>();
            for (NodeContext<E> nodeContext : this.queue) {
                if (nodeContext.e != null) {
                    elements.add(nodeContext.e);
                }
            }
            this.queue.clear();
            this.pendingIndex = 0;
            return elements;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean add(@Nullable E e) {
        this.writeLock.lock();
        try {
            return queue.add(new NodeContext<>(e));
        } finally {
            this.writeLock.unlock();
        }
    }

    @Nullable
    @Override
    public E poll() {
        this.writeLock.lock();
        try {
            final NodeContext<E> nodeContext = queue.poll();
            if (nodeContext != null) {
                advancePendingIndex();
                return nodeContext.e;
            }
            return null;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public E poll(long index) {
        this.writeLock.lock();
        try {
            if (index == this.pendingIndex) {
                return poll();
            }
        } finally {
            this.writeLock.unlock();
        }
        return null;
    }

    @Override
    public List<E> pollUntil(final long endIndex) {
        this.writeLock.lock();
        try {
            List<E> list = new ArrayList<>();
            while (!this.isEmpty() && this.pendingIndex <= endIndex) {
                list.add(poll());
            }
            return list;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Nullable
    @Override
    public E peek() {
        this.readLock.lock();
        try {
            final NodeContext<E> nodeContext = this.queue.peek();
            if (nodeContext != null) {
                return nodeContext.e;
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public int size() {
        this.readLock.lock();
        try {
            return queue.size();
        } finally {
            this.readLock.unlock();
        }
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
