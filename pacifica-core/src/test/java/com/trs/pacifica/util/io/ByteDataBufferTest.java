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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.util.Arrays;
import java.util.List;

public class ByteDataBufferTest {
    ByteDataBuffer byteDataBuffer = new ByteDataBuffer(new byte[]{(byte) 0}, 0, 0);

    @Test
    void test() {
        final int capacity = 10;
        byte[] bytes = new byte[capacity];
        ByteDataBuffer byteDataBuffer = new ByteDataBuffer(bytes);
        int result = byteDataBuffer.capacity();
        Assertions.assertEquals(capacity, result);
        int limit = byteDataBuffer.limit();
        Assertions.assertEquals(capacity, limit);
        int pos = byteDataBuffer.position();
        Assertions.assertEquals(0, pos);

        final int offset = 2;
        byteDataBuffer = new ByteDataBuffer(bytes, offset);
        result = byteDataBuffer.capacity();
        Assertions.assertEquals(capacity - offset, result);
        limit = byteDataBuffer.limit();
        Assertions.assertEquals(capacity - offset, limit);
        pos = byteDataBuffer.position();
        Assertions.assertEquals(0, pos);

    }

    @Test
    void testGet() {
        final int len = 10;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)i;
        }
        ByteDataBuffer byteDataBuffer = new ByteDataBuffer(bytes);
        byte result = byteDataBuffer.get();
        Assertions.assertEquals((byte) 0, result);
        result = byteDataBuffer.get();
        Assertions.assertEquals((byte) 1, result);
        byteDataBuffer.position(9);
        result = byteDataBuffer.get();
        Assertions.assertEquals((byte) 9, result);

        BufferUnderflowException error = null;
        try {
            result = byteDataBuffer.get();
        } catch (BufferUnderflowException e) {
            error = e;
        }
        Assertions.assertNotNull(error);

    }

    @Test
    void testSlice() {

        final int len = 10;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)i;
        }
        ByteDataBuffer byteDataBuffer = new ByteDataBuffer(bytes);
        DataBuffer result = byteDataBuffer.slice();
        Assertions.assertEquals(0, result.position());
        Assertions.assertEquals(10, result.capacity());
        Assertions.assertEquals(10, result.limit());

    }

    @Test
    void testSlice2() {
        final int len = 10;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)i;
        }
        ByteDataBuffer byteDataBuffer = new ByteDataBuffer(bytes);
        DataBuffer result = byteDataBuffer.slice(1, 7);
        Assertions.assertEquals(0, result.position());
        Assertions.assertEquals(7, result.capacity());
        Assertions.assertEquals(7, result.limit());
    }


    @Test
    void testRemaining() {
        final int len = 10;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)i;
        }
        ByteDataBuffer byteDataBuffer = new ByteDataBuffer(bytes);
        byteDataBuffer.position(2);
        int result = byteDataBuffer.remaining();
        Assertions.assertEquals(8, result);
    }


    @Test
    void testReadRemain() {
        final int len = 10;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)i;
        }
        ByteDataBuffer byteDataBuffer = new ByteDataBuffer(bytes);
        byteDataBuffer.position(2);
        byte[] remainBytes = byteDataBuffer.readRemain();
        Assertions.assertEquals(8, remainBytes.length);

        byte[] ranges = Arrays.copyOfRange(bytes, 2, 10);

        Assertions.assertArrayEquals(ranges, remainBytes);


    }

}