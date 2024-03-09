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

public class BitUtil {

    private BitUtil() {
    }


    /**
     * Same as {@link #zigZagEncode(long)} but on integers.
     */
    public static int zigZagEncode(int i) {
        return (i >> 31) ^ (i << 1);
    }

    /**
     * <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">Zig-zag</a> encode
     * the provided long. Assuming the input is a signed long whose absolute value can be stored on
     * <code>n</code> bits, the returned value will be an unsigned long that can be stored on <code>
     * n+1</code> bits.
     */
    public static long zigZagEncode(long l) {
        return (l >> 63) ^ (l << 1);
    }

    /**
     * Decode an int previously encoded with {@link #zigZagEncode(int)}.
     */
    public static int zigZagDecode(int i) {
        return ((i >>> 1) ^ -(i & 1));
    }

    /**
     * Decode a long previously encoded with {@link #zigZagEncode(long)}.
     */
    public static long zigZagDecode(long l) {
        return ((l >>> 1) ^ -(l & 1));
    }



    /** Maximum number of UTF8 bytes per UTF16 character. */
    public static final int MAX_UTF8_BYTES_PER_CHAR = 3;

    /**
     * Returns the maximum number of utf8 bytes required to encode a utf16 (e.g., java char[], String)
     */
    public static int maxUTF8Length(int utf16Length) {
        return Math.multiplyExact(utf16Length, MAX_UTF8_BYTES_PER_CHAR);
    }


    public static int getInt(final byte[] b, final int off) {
        return HeapByteBufUtil.getInt(b, off);
    }

    public static long getLong(final byte[] b, final int off) {
        return HeapByteBufUtil.getLong(b, off);
    }

    public static void putInt(final byte[] b, final int off, final int val) {
        HeapByteBufUtil.setInt(b, off, val);
    }

    public static void putShort(final byte[] b, final int off, final short val) {
        HeapByteBufUtil.setShort(b, off, val);
    }

    public static short getShort(final byte[] b, final int off) {
        return HeapByteBufUtil.getShort(b, off);
    }

    public static void putLong(final byte[] b, final int off, final long val) {
        HeapByteBufUtil.setLong(b, off, val);
    }

}
