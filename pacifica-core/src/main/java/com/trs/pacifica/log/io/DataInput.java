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

package com.trs.pacifica.log.io;

import com.trs.pacifica.util.BitUtil;

import java.io.IOException;
import java.util.Objects;

/**
 * code from lucene
 */
public class DataInput implements Input {

    private final Input input;

    public DataInput(Input input) {
        this.input = input;
    }


    /**
     * Reads two bytes and returns a short (LE byte order).
     *
     * @see DataOutput#writeShort(short)
     */
    public short readShort() throws IOException {
        final byte b1 = readByte();
        final byte b2 = readByte();
        return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
    }

    /**
     * Reads four bytes and returns an int (LE byte order).
     *
     * @see DataOutput#writeInt(int)
     */
    public int readInt() throws IOException {
        final byte b1 = readByte();
        final byte b2 = readByte();
        final byte b3 = readByte();
        final byte b4 = readByte();
        return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
    }

    /**
     * Reads an int stored in variable-length format. Reads between one and five bytes. Smaller values
     * take fewer bytes. Negative numbers are supported, but should be avoided.
     *
     * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
     *
     * @see DataOutput#writeVInt(int)
     */
    public int readVInt() throws IOException {
        byte b = readByte();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    /**
     * Read a {@link BitUtil#zigZagDecode(int) zig-zag}-encoded {@link #readVInt() variable-length}
     * integer.
     *
     * @see DataOutput#writeZInt(int)
     */
    public int readZInt() throws IOException {
        return BitUtil.zigZagDecode(readVInt());
    }

    /**
     * Reads eight bytes and returns a long (LE byte order).
     *
     * @see DataOutput#writeLong(long)
     */
    public long readLong() throws IOException {
        return (readInt() & 0xFFFFFFFFL) | (((long) readInt()) << 32);
    }

    /**
     * Read a specified number of longs.
     *
     * @lucene.experimental
     */
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, dst.length);
        for (int i = 0; i < length; ++i) {
            dst[offset + i] = readLong();
        }
    }

    /**
     * Reads a specified number of ints into an array at the specified offset.
     *
     * @param dst    the array to read bytes into
     * @param offset the offset in the array to start storing ints
     * @param length the number of ints to read
     */
    public void readInts(int[] dst, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, dst.length);
        for (int i = 0; i < length; ++i) {
            dst[offset + i] = readInt();
        }
    }

    /**
     * Reads a specified number of floats into an array at the specified offset.
     *
     * @param floats the array to read bytes into
     * @param offset the offset in the array to start storing floats
     * @param len    the number of floats to read
     */
    public void readFloats(float[] floats, int offset, int len) throws IOException {
        Objects.checkFromIndexSize(offset, len, floats.length);
        for (int i = 0; i < len; i++) {
            floats[offset + i] = Float.intBitsToFloat(readInt());
        }
    }

    /**
     * Reads a long stored in variable-length format. Reads between one and nine bytes. Smaller values
     * take fewer bytes. Negative numbers are not supported.
     *
     * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
     *
     * @see DataOutput#writeVLong(long)
     */
    public long readVLong() throws IOException {
        byte b = readByte();
        long i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7FL) << shift;
        }
        return i;
    }

    /**
     * Read a {@link BitUtil#zigZagDecode(long) zig-zag}-encoded {@link #readVLong() variable-length}
     * integer. Reads between one and ten bytes.
     *
     * @see DataOutput#writeZLong(long)
     */
    public long readZLong() throws IOException {
        return BitUtil.zigZagDecode(readVLong());
    }

    @Override
    public byte readByte() throws IOException {
        return this.input.readByte();
    }

    @Override
    public void seek(long pos) throws IOException {
        this.input.seek(pos);
    }
}
