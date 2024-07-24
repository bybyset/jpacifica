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

import java.nio.BufferUnderflowException;

public class EmptyDataBuffer implements DataBuffer {
    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public int position() {
        return 0;
    }

    @Override
    public DataBuffer position(int newPosition) {
        return this;
    }

    @Override
    public int limit() {
        return 0;
    }

    @Override
    public DataBuffer limit(int newLimit) {
        return this;
    }

    @Override
    public DataBuffer mark() {
        return this;
    }

    @Override
    public DataBuffer clear() {
        return this;
    }

    @Override
    public byte get() {
        throw new BufferUnderflowException();
    }

    @Override
    public byte get(int index) {
        throw new BufferUnderflowException();
    }

    @Override
    public DataBuffer get(byte[] dst, int offset, int length) {
        return this;
    }

    @Override
    public DataBuffer reset() {
        return this;
    }

    @Override
    public DataBuffer slice() {
        return this;
    }

    @Override
    public DataBuffer slice(int index, int length) {
        return this;
    }
}
