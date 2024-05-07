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

package com.trs.pacifica.util.io;

import java.nio.InvalidMarkException;

public abstract class AbstractDataBuffer implements DataBuffer {

    // Invariants: mark <= position <= limit <= capacity
    protected int mark = -1;

    protected int position = 0;

    protected int limit = 0;

    @Override
    public final int position() {
        return position;
    }

    @Override
    public DataBuffer position(int newPosition) {
        if (newPosition > limit | newPosition < 0)
            throw createPositionException(newPosition);
        position = newPosition;
        return this;
    }

    @Override
    public int limit() {
        return limit;
    }

    /**
     * Sets this buffer's limit.  If the position is larger than the new limit
     * then it is set to the new limit.  If the mark is defined and larger than
     * the new limit then it is discarded.
     *
     * @param newLimit The new limit value; must be non-negative
     *                 and no larger than this buffer's capacity
     * @return This buffer
     * @throws IllegalArgumentException If the preconditions on {@code newLimit} do not hold
     */
    public DataBuffer limit(int newLimit) {
        if (newLimit > capacity() | newLimit < 0)
            throw createLimitException(newLimit);
        limit = newLimit;
        if (position > newLimit) position = newLimit;
        return this;
    }

    @Override
    public DataBuffer mark() {
        mark = position;
        return this;
    }


    @Override
    public DataBuffer clear() {
        position = 0;
        limit = capacity();
        mark = -1;
        return this;
    }

    @Override
    public DataBuffer reset() {
        int m = mark;
        if (m < 0)
            throw new InvalidMarkException();
        position = m;
        return this;
    }

    /**
     * Verify that {@code 0 < newPosition <= limit}
     *
     * @param newPosition The new position value
     * @throws IllegalArgumentException If the specified position is out of bounds.
     */
    private IllegalArgumentException createPositionException(int newPosition) {
        String msg = null;
        if (newPosition > limit) {
            msg = "newPosition > limit: (" + newPosition + " > " + limit + ")";
        } else { // assume negative
            assert newPosition < 0 : "newPosition expected to be negative";
            msg = "newPosition < 0: (" + newPosition + " < 0)";
        }
        return new IllegalArgumentException(msg);
    }

    /**
     * Verify that {@code 0 < newLimit <= capacity}
     *
     * @param newLimit The new limit value
     * @throws IllegalArgumentException If the specified limit is out of bounds.
     */
    private IllegalArgumentException createLimitException(int newLimit) {
        String msg = null;

        if (newLimit > capacity()) {
            msg = "newLimit > capacity: (" + newLimit + " > " + capacity() + ")";
        } else { // assume negative
            assert newLimit < 0 : "newLimit expected to be negative";
            msg = "newLimit < 0: (" + newLimit + " < 0)";
        }

        return new IllegalArgumentException(msg);
    }

}
