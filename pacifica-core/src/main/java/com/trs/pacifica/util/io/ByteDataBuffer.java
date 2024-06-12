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
import java.util.Objects;

public class ByteDataBuffer extends AbstractDataBuffer{

    private final byte[] bytes;

    private final int offset;

    private final int length;

    public ByteDataBuffer(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public ByteDataBuffer(byte[] bytes, int offset) {
        this(bytes, offset, bytes.length - offset);
    }

    public ByteDataBuffer(byte[] bytes, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.limit(length);
        this.position(0);
    }

    @Override
    public int capacity() {
        return this.length;
    }


    @Override
    public byte get() {
        int pos = position();
        if (pos >= limit()) {
            throw new BufferUnderflowException();
        }
        return bytes[offset + this.position++];
    }

    @Override
    public byte get(int index) {
        if (index < 0 || index > limit()) {
            throw new IndexOutOfBoundsException(String.format("index(%d) is less than 0 or greater than limit(%d).", index, limit()));
        }
        return bytes[offset + index];
    }

    @Override
    public DataBuffer get(byte[] dst, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, dst.length);
        int pos = position();
        if (length > limit() - pos)
            throw new BufferUnderflowException();
        System.arraycopy(bytes, this.offset + position(), dst, offset, length);
        position(pos + length);
        return this;
    }


    @Override
    public DataBuffer slice() {
        return new ByteDataBuffer(this.bytes, this.offset, this.length);
    }

    @Override
    public DataBuffer slice(int index, int length) {
        return new ByteDataBuffer(this.bytes, this.offset + index, length);
    }
}
