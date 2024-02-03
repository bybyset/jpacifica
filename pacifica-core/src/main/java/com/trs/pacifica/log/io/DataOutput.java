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

/**
 *
 */
public class DataOutput implements Output {


    private final Output output;

    public DataOutput(final Output output) {
        this.output = output;
    }

    /**
     * Writes an int as four bytes (LE byte order).
     *
     * @see DataInput#readInt()
     */
    public void writeInt(int i) throws IOException {
        writeByte((byte) i);
        writeByte((byte) (i >> 8));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 24));
    }

    /**
     * Writes a short as two bytes (LE byte order).
     *
     * @see DataInput#readShort()
     */
    public void writeShort(short i) throws IOException {
        writeByte((byte) i);
        writeByte((byte) (i >> 8));
    }

    /**
     * Writes an int in a variable-length format. Writes between one and five bytes. Smaller values
     * take fewer bytes. Negative numbers are supported, but should be avoided.
     *
     * <p>VByte is a variable-length format for positive integers is defined where the high-order bit
     * of each byte indicates whether more bytes remain to be read. The low-order seven bits are
     * appended as increasingly more significant bits in the resulting integer value. Thus values from
     * zero to 127 may be stored in a single byte, values from 128 to 16,383 may be stored in two
     * bytes, and so on.
     *
     * <p>VByte Encoding Example
     *
     * <table class="padding2" style="border-spacing: 0px; border-collapse: separate; border: 0">
     * <caption>variable length encoding examples</caption>
     * <tr style="vertical-align: top">
     *   <th style="text-align:left">Value</th>
     *   <th style="text-align:left">Byte 1</th>
     *   <th style="text-align:left">Byte 2</th>
     *   <th style="text-align:left">Byte 3</th>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>0</td>
     *   <td><code>00000000</code></td>
     *   <td></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>1</td>
     *   <td><code>00000001</code></td>
     *   <td></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>2</td>
     *   <td><code>00000010</code></td>
     *   <td></td>
     *   <td></td>
     * </tr>
     * <tr>
     *   <td style="vertical-align: top">...</td>
     *   <td></td>
     *   <td></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>127</td>
     *   <td><code>01111111</code></td>
     *   <td></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>128</td>
     *   <td><code>10000000</code></td>
     *   <td><code>00000001</code></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>129</td>
     *   <td><code>10000001</code></td>
     *   <td><code>00000001</code></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>130</td>
     *   <td><code>10000010</code></td>
     *   <td><code>00000001</code></td>
     *   <td></td>
     * </tr>
     * <tr>
     *   <td style="vertical-align: top">...</td>
     *   <td></td>
     *   <td></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>16,383</td>
     *   <td><code>11111111</code></td>
     *   <td><code>01111111</code></td>
     *   <td></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>16,384</td>
     *   <td><code>10000000</code></td>
     *   <td><code>10000000</code></td>
     *   <td><code>00000001</code></td>
     * </tr>
     * <tr style="vertical-align: bottom">
     *   <td>16,385</td>
     *   <td><code>10000001</code></td>
     *   <td><code>10000000</code></td>
     *   <td><code>00000001</code></td>
     * </tr>
     * <tr>
     *   <td style="vertical-align: top">...</td>
     *   <td ></td>
     *   <td ></td>
     *   <td ></td>
     * </tr>
     * </table>
     *
     * <p>This provides compression while still being efficient to decode.
     *
     * <p>This provides compression while still being efficient to decode.
     *
     * @param i Smaller values take fewer bytes. Negative numbers are supported, but should be
     *          avoided.
     * @throws IOException If there is an I/O error writing to the underlying medium.
     * @see DataInput#readVInt()
     */
    public final void writeVInt(int i) throws IOException {
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    /**
     * Write a {@link BitUtil#zigZagEncode(int) zig-zag}-encoded {@link #writeVInt(int)
     * variable-length} integer. This is typically useful to write small signed ints and is equivalent
     * to calling <code>writeVInt(BitUtil.zigZagEncode(i))</code>.
     *
     * @see DataInput#readZInt()
     */
    public final void writeZInt(int i) throws IOException {
        writeVInt(BitUtil.zigZagEncode(i));
    }

    /**
     * Writes a long as eight bytes (LE byte order).
     *
     * @see DataInput#readLong()
     */
    public void writeLong(long i) throws IOException {
        writeInt((int) i);
        writeInt((int) (i >> 32));
    }

    /**
     * Writes an long in a variable-length format. Writes between one and nine bytes. Smaller values
     * take fewer bytes. Negative numbers are not supported.
     *
     * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
     *
     * @see DataInput#readVLong()
     */
    public final void writeVLong(long i) throws IOException {
        if (i < 0) {
            throw new IllegalArgumentException("cannot write negative vLong (got: " + i + ")");
        }
        writeSignedVLong(i);
    }

    // write a potentially negative vLong
    private void writeSignedVLong(long i) throws IOException {
        while ((i & ~0x7FL) != 0L) {
            writeByte((byte) ((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    /**
     * Write a {@link BitUtil#zigZagEncode(long) zig-zag}-encoded {@link #writeVLong(long)
     * variable-length} long. Writes between one and ten bytes. This is typically useful to write
     * small signed ints.
     *
     * @see DataInput#readZLong()
     */
    public final void writeZLong(long i) throws IOException {
        writeSignedVLong(BitUtil.zigZagEncode(i));
    }

    @Override
    public void writeByte(byte b) throws IOException {
        this.output.writeByte(b);
    }
}
