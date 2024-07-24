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

import com.trs.pacifica.util.ObjectsUtil;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

public interface Input extends Closeable {

    /**
     * Reads and returns a single byte.
     *
     * @throws EOFException If this is beyond the end of the file
     * @throws IOException  in case of I/O error
     */
    byte readByte() throws IOException;

    /**
     * Reads a specified number of bytes into an array.
     *
     * @param b   the array to read bytes into
     * @param off the start offset in array b at which the data is written.
     * @param len the number of bytes to read
     * @return the total number of bytes read into the buffer,
     * or -1 if there is no more data because the end of the stream has been reached.
     * @throws IOException               in case of I/O error
     * @throws NullPointerException      if b is null
     * @throws IndexOutOfBoundsException If {@code off} is negative,
     *                                   {@code len} is negative, or {@code len} is greater than
     *                                   {@code b.length - off}
     */
    default int readBytes(byte[] b, int off, int len) throws IOException {
        ObjectsUtil.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        int i = 0;
        try {
            for (; i < len; i++) {
                byte c = readByte();
                b[off + i] = c;
            }
        } catch (EOFException eof) {

        }
        return i == 0 ? -1 : i;
    }

    /**
     * Reads a specified number of bytes into an array.
     *
     * @param b the array to read bytes into
     * @throws IOException          in case of I/O error
     * @throws NullPointerException if b is null
     * @see #readBytes(byte[], int, int)
     */
    default int readBytes(byte[] b) throws IOException {
        return readBytes(b, 0, b.length);
    }

    /**
     * Sets current position in this file, where the next read will occur. If this is beyond the end
     * of the file then this will throw {@code EOFException} and then the stream is in an undetermined
     * state.
     * @throws EOFException
     */
    void seek(long pos) throws IOException;


}
