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

import java.io.Closeable;
import java.io.IOException;

public interface Output extends Closeable {

    /**
     * Writes a single byte.
     */
    public void writeByte(byte b) throws IOException;


    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     */
    default public void writeBytes(byte[] b) throws IOException {
        writeBytes(b, 0, b.length);
    }

    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     * @param length the number of bytes to write
     */
    default public void writeBytes(byte[] b, int length) throws IOException {
        writeBytes(b, 0, length);
    }


    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     * @param offset the offset in the byte array
     * @param length the number of bytes to write
     */
    default public void writeBytes(byte[] b, int offset, int length) throws IOException {
        for (;offset < b.length || offset < length ;offset++) {
            writeByte(b[offset]);
        }
    }

    /**
     *
     * @param index The index in this output at which the first byte will be written.
     * @param bytes
     * @throws IOException
     */
    default public void writeBytes(final int index, final byte[] bytes) throws IOException {
        writeBytes(index, bytes, 0, bytes.length);
    }


    /**
     *
     * @param index
     * @param bytes
     * @throws IOException
     */
    public void writeBytes(final int index, final byte[] bytes, final int offset, final int length) throws IOException;

    public void flush() throws IOException;



}
