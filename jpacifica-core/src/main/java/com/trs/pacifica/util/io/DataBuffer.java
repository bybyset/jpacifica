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

import com.trs.pacifica.error.NotSupportedException;

import java.nio.BufferUnderflowException;

public interface DataBuffer {

    /**
     * Returns this buffer's capacity.
     *
     * @return The capacity of this buffer
     */
    public abstract int capacity();

    /**
     * Returns this buffer's position.
     *
     * @return The position of this buffer
     */
    public abstract int position();

    /**
     * Sets this buffer's position.  If the mark is defined and larger than the
     * new position then it is discarded.
     *
     * @param newPosition The new position value; must be non-negative
     *                    and no larger than the current limit
     * @return This buffer
     * @throws IllegalArgumentException If the preconditions on {@code newPosition} do not hold
     */
    public DataBuffer position(int newPosition);

    /**
     * Returns this buffer's limit.
     *
     * @return The limit of this buffer
     */
    public abstract int limit();

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
    public abstract DataBuffer limit(int newLimit);

    /**
     * Sets this buffer's mark at its position.
     *
     * @return This buffer
     */
    public abstract DataBuffer mark();

    /**
     * Clears this buffer.  The position is set to zero, the limit is set to
     * the capacity, and the mark is discarded.
     *
     * <p> Invoke this method before using a sequence of channel-read or
     * <i>put</i> operations to fill this buffer.  For example:
     *
     * <blockquote><pre>
     * buf.clear();     // Prepare buffer for reading
     * in.read(buf);    // Read data</pre></blockquote>
     *
     * <p> This method does not actually erase the data in the buffer, but it
     * is named as if it did because it will most often be used in situations
     * in which that might as well be the case. </p>
     *
     * @return This buffer
     *
     */
    public abstract DataBuffer clear();

    /**
     * Tells whether there are any elements between the current position and
     * the limit.
     *
     * @return  {@code true} if, and only if, there is at least one element
     *          remaining in this buffer
     */
    public default boolean hasRemaining() {
        return position() < limit();
    }


    /**
     * Returns the number of elements between the current position and the limit.
     * @return The number of elements remaining in this buffer
     */
    public default int remaining() {
        return limit() - position();
    }

    /**
     * Relative <i>get</i> method.  Reads the byte at this buffer's
     * current position, and then increments the position.
     *
     * @return The byte at the buffer's current position
     * @throws BufferUnderflowException If the buffer's current position is not smaller than its limit
     */
    public abstract byte get();

    /**
     * Absolute <i>get</i> method.  Reads the byte at the given
     * index.
     *
     * @param index The index from which the byte will be read
     * @return The byte at the given index
     * @throws IndexOutOfBoundsException If {@code index} is negative
     *                                   or not smaller than the buffer's limit
     */
    public abstract byte get(int index);


    // -- Bulk get operations --

    /**
     * Relative bulk <i>get</i> method.
     *
     * <p> This method transfers bytes from this buffer into the given
     * destination array.  If there are fewer bytes remaining in the
     * buffer than are required to satisfy the request, that is, if
     * {@code length}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
     * bytes are transferred and a {@link BufferUnderflowException} is
     * thrown.
     *
     * <p> Otherwise, this method copies {@code length} bytes from this
     * buffer into the given array, starting at the current position of this
     * buffer and at the given offset in the array.  The position of this
     * buffer is then incremented by {@code length}.
     *
     * <p> In other words, an invocation of this method of the form
     * <code>src.get(dst,&nbsp;off,&nbsp;len)</code> has exactly the same effect as
     * the loop
     *
     * <pre>{@code
     *     for (int i = off; i < off + len; i++)
     *         dst[i] = src.get();
     * }</pre>
     * <p>
     * except that it first checks that there are sufficient bytes in
     * this buffer and it is potentially much more efficient.
     *
     * @param dst    The array into which bytes are to be written
     * @param offset The offset within the array of the first byte to be
     *               written; must be non-negative and no larger than
     *               {@code dst.length}
     * @param length The maximum number of bytes to be written to the given
     *               array; must be non-negative and no larger than
     *               {@code dst.length - offset}
     * @return This buffer
     * @throws BufferUnderflowException  If there are fewer than {@code length} bytes
     *                                   remaining in this buffer
     * @throws IndexOutOfBoundsException If the preconditions on the {@code offset} and {@code length}
     *                                   parameters do not hold
     */
    public DataBuffer get(byte[] dst, int offset, int length);


    /**
     * Relative bulk <i>get</i> method.
     *
     * <p> This method transfers bytes from this buffer into the given
     * destination array.  An invocation of this method of the form
     * {@code src.get(a)} behaves in exactly the same way as the invocation
     *
     * <pre>
     *     src.get(a, 0, a.length) </pre>
     *
     * @param dst The destination array
     * @return This buffer
     * @throws BufferUnderflowException If there are fewer than {@code length} bytes
     *                                  remaining in this buffer
     */
    public default DataBuffer get(byte[] dst) {
        return get(dst, 0, dst.length);
    }

    public abstract DataBuffer reset();

    /**
     * Creates a new buffer whose content is a shared subsequence of
     * this buffer's content.
     *
     * <p> The content of the new buffer will start at this buffer's current
     * position.  Changes to this buffer's content will be visible in the new
     * buffer, and vice versa; the two buffers' position, limit, and mark
     * values will be independent.
     *
     * <p> The new buffer's position will be zero, its capacity and its limit
     * will be the number of elements remaining in this buffer, its mark will be
     * undefined. The new buffer will be direct if, and only if, this buffer is
     * direct, and it will be read-only if, and only if, this buffer is
     * read-only.  </p>
     *
     * @return The new buffer
     * @throws NotSupportedException If not support
     * @since 9
     */
    public abstract DataBuffer slice();

    /**
     * Creates a new buffer whose content is a shared subsequence of
     * this buffer's content.
     *
     * <p> The content of the new buffer will start at position {@code index}
     * in this buffer, and will contain {@code length} elements. Changes to
     * this buffer's content will be visible in the new buffer, and vice versa;
     * the two buffers' position, limit, and mark values will be independent.
     *
     * <p> The new buffer's position will be zero, its capacity and its limit
     * will be {@code length}, its mark will be undefined. The new buffer will
     * be direct if, and only if, this buffer is direct, and it will be
     * read-only if, and only if, this buffer is read-only.  </p>
     *
     * @param index  The position in this buffer at which the content of the new
     *               buffer will start; must be non-negative and no larger than
     *               {@link #limit() limit()}
     * @param length The number of elements the new buffer will contain; must be
     *               non-negative and no larger than {@code limit() - index}
     * @return The new buffer
     * @throws IndexOutOfBoundsException If {@code index} is negative or greater than {@code limit()},
     *                                   {@code length} is negative, or {@code length > limit() - index}
     * @throws NotSupportedException If not support
     * @since 13
     */
    public abstract DataBuffer slice(int index, int length);


    default byte[] readRemain() {
        int remaining = remaining();
        if (remaining > 0) {
            byte[] bytes = new byte[remaining];
            get(bytes);
            return bytes;
        }
        return new byte[0];
    }

}
