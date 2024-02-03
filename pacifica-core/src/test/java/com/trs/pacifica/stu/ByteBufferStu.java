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

package com.trs.pacifica.stu;


import org.junit.Test;

import java.nio.ByteBuffer;

public class ByteBufferStu {


    @Test
    public void testReadOnly() {

        final byte[] common = "hello".getBytes();
        printBytes(common);
        ByteBuffer byteBuffer = ByteBuffer.wrap(common);
        final ByteBuffer readOnly = byteBuffer.asReadOnlyBuffer();
        printByteBuffer(byteBuffer);
        byteBuffer = byteBuffer.duplicate();
        byteBuffer.position(2);
        byteBuffer.put((byte) 0x30);
        printByteBuffer(readOnly);
        printByteBuffer(byteBuffer);


    }

    static void printByteBuffer(ByteBuffer byteBuffer) {
        System.out.printf(byteBuffer.toString());
        System.out.printf(":");
        while (byteBuffer.hasRemaining()) {
            System.out.printf(byteBuffer.get() + " ");
        }
        System.out.println();
    }

    static void printBytes(byte[] bytes) {
        for (byte v : bytes) {
            System.out.printf(v + " ");
        }
        System.out.println();
    }


}
