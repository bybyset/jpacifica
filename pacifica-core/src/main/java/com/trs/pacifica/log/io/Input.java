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
import java.io.EOFException;
import java.io.IOException;

public interface Input extends Closeable {

    /**
     * Reads and returns a single byte.
     *
     * @throws EOFException If this is beyond the end of the file
     */
    public abstract byte readByte() throws IOException;

    /**
     * Reads a specified number of bytes into an array.
     *
     * @param b   the array to read bytes into
     * @param len the number of bytes to read
     * @throws EOFException If this is beyond the end of the file
     * @throws IOException in case of I/O error
     */
    default public void readBytes(byte[] b, int len) throws IOException {
        for (int i = 0; i < len; i++) {
            b[i] = readByte();
        }
    }

    /**
     * Reads a specified number of bytes into an array.
     *
     * @param b the array to read bytes into
     * @throws EOFException If this is beyond the end of the file
     * @throws IOException in case of I/O error
     */
    default public void readBytes(byte[] b) throws IOException {
        readBytes(b, b.length);
    }

    /**
     * Sets current position in this file, where the next read will occur. If this is beyond the end
     * of the file then this will throw {@code EOFException} and then the stream is in an undetermined
     * state.
     */
    public abstract void seek(long pos) throws IOException;


}
