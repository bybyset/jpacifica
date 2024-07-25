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

package com.trs.pacifica.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public class LinkedBuffer {

    /**
     * The minimum buffer size for a {@link LinkedBuffer}.
     */
    public static final int MIN_BUFFER_SIZE = 256;

    /**
     * The default buffer size for a {@link LinkedBuffer}.
     */
    public static final int DEFAULT_BUFFER_SIZE = 512;

    /**
     * Allocates a new buffer with default size.
     *
     * @return LinkedBuffer
     */
    public static LinkedBuffer allocate() {
        return new LinkedBuffer(DEFAULT_BUFFER_SIZE);
    }

    /**
     * Allocates a new buffer with the specified size.
     *
     * @param size size
     * @return LinkedBuffer
     */
    public static LinkedBuffer allocate(int size) {
        if (size < MIN_BUFFER_SIZE)
            throw new IllegalArgumentException(MIN_BUFFER_SIZE + " is the minimum buffer size.");

        return new LinkedBuffer(size);
    }

    /**
     * Allocates a new buffer with the specified size and appends it to the previous buffer.
     *
     * @param size     size
     * @param previous previous
     * @return LinkedBuffer
     */
    public static LinkedBuffer allocate(int size, LinkedBuffer previous) {
        if (size < MIN_BUFFER_SIZE)
            throw new IllegalArgumentException(MIN_BUFFER_SIZE + " is the minimum buffer size.");

        return new LinkedBuffer(size, previous);
    }

    /**
     * Wraps the byte array buffer as a read-only buffer.
     *
     * @param array  array
     * @param offset offset
     * @param length length
     * @return LinkedBuffer
     */
    public static LinkedBuffer wrap(byte[] array, int offset, int length) {
        return new LinkedBuffer(array, offset, offset + length);
    }

    /**
     * Uses the existing byte array as the internal buffer.
     *
     * @param buffer buffer
     * @return LinkedBuffer
     */
    public static LinkedBuffer use(byte[] buffer) {
        return use(buffer, 0);
    }

    /**
     * Uses the existing byte array as the internal buffer.
     *
     * @param buffer buffer
     * @param start  start
     * @return LinkedBuffer
     */
    public static LinkedBuffer use(byte[] buffer, int start) {
        assert start >= 0;
        if (buffer.length - start < MIN_BUFFER_SIZE)
            throw new IllegalArgumentException(MIN_BUFFER_SIZE + " is the minimum buffer size.");

        return new LinkedBuffer(buffer, start, start);
    }

    /**
     * Writes the contents of the {@link LinkedBuffer} into the {@link OutputStream}.
     *
     * @param out  out
     * @param node node
     * @return the total content size of the buffer.
     * @throws IOException io error
     */
    public static int writeTo(final OutputStream out, LinkedBuffer node) throws IOException {
        int contentSize = 0, len;
        do {
            if ((len = node.offset - node.start) > 0) {
                out.write(node.buffer, node.start, len);
                contentSize += len;
            }
        } while ((node = node.next) != null);

        return contentSize;
    }

    /**
     * Writes the contents of the {@link LinkedBuffer} into the {@link DataOutput}.
     *
     * @param out  out
     * @param node node
     * @return the total content size of the buffer.
     * @throws IOException io error
     */
    public static int writeTo(final DataOutput out, LinkedBuffer node) throws IOException {
        int contentSize = 0, len;
        do {
            if ((len = node.offset - node.start) > 0) {
                out.write(node.buffer, node.start, len);
                contentSize += len;
            }
        } while ((node = node.next) != null);

        return contentSize;
    }

    final byte[] buffer;

    final int start;

    int offset;

    LinkedBuffer next;

    /**
     * Creates a buffer with the specified {@code size}.
     *
     * @param size size
     */
    LinkedBuffer(int size) {
        this(new byte[size], 0, 0);
    }

    /**
     * Creates a buffer with the specified {@code size} and appends to the provided buffer {@code appendTarget}.
     *
     * @param size         size
     * @param appendTarget appendTarget
     */
    LinkedBuffer(int size, LinkedBuffer appendTarget) {
        this(new byte[size], 0, 0, appendTarget);
    }

    /**
     * Uses the buffer starting at the specified {@code offset}
     *
     * @param buffer buffer
     * @param offset offset
     */
    LinkedBuffer(byte[] buffer, int offset) {
        this(buffer, offset, offset);
    }

    LinkedBuffer(byte[] buffer, int start, int offset) {
        this.buffer = buffer;
        this.start = start;
        this.offset = offset;
    }

    /**
     * Uses the buffer starting at the specified {@code offset} and appends to the provided buffer {@code appendTarget}.
     *
     * @param buffer       buffer
     * @param offset       offset
     * @param appendTarget appendTarget
     */
    LinkedBuffer(byte[] buffer, int offset, LinkedBuffer appendTarget) {
        this(buffer, offset, offset);
        appendTarget.next = this;
    }

    LinkedBuffer(byte[] buffer, int start, int offset, LinkedBuffer appendTarget) {
        this(buffer, start, offset);
        appendTarget.next = this;
    }

    /**
     * Creates a view from the buffer {@code viewSource} and appends the view to the provided buffer
     * {@code appendTarget}.
     *
     * @param appendTarget appendTarget
     * @param viewSource   viewSource
     */
    LinkedBuffer(LinkedBuffer viewSource, LinkedBuffer appendTarget) {
        buffer = viewSource.buffer;
        offset = start = viewSource.offset;
        appendTarget.next = this;
    }

    /**
     * The offset will be reset to its starting position. The buffer next to this will be dereferenced.
     *
     * @return LinkedBuffer
     */
    public LinkedBuffer clear() {
        next = null;
        offset = start;
        return this;
    }

}
