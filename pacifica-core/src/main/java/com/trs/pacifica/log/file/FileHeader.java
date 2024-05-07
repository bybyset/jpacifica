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

package com.trs.pacifica.log.file;

import com.trs.pacifica.util.io.ByteDataBuffer;
import com.trs.pacifica.util.io.DataBuffer;

import java.nio.ByteBuffer;

/**
 * HEADER:
 * <pre>
 * --------------+---------+--------------+-----------------+
 * | magic byte | reserved | start offset | first log index |
 * |[0x20][0x20]|[8 bytes] | [ 8   bytes ]| [    8  bytes  ]|
 * -------------+------ ---+---------- ---+-----------------+
 * </pre>
 */
public class FileHeader {

    static final int _HEADER_BYTE_SIZE = 26;

    private static final byte _MAGIC = 0X20;

    private static final byte TAG_BLANK = 0x00;

    /**
     * Mark that the file contains at least the beginning of a complete log
     */
    private static final byte TAG_AVAILABLE = 1;

    /**
     *
     */
    private static final byte TAG_CONSECUTIVE =  1 << 1;


    private final byte magic = _MAGIC;

    private byte tag = TAG_BLANK;

    private long firstLogIndex = 0L;

    private long startOffset = 0L;

    public long getFirstLogIndex() {
        return firstLogIndex;
    }

    public void setFirstLogIndex(long firstLogIndex) {
        this.firstLogIndex = firstLogIndex;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }


    public byte[] encode() {
        final ByteBuffer headerData = ByteBuffer.allocate(_HEADER_BYTE_SIZE);
        headerData.put(magic);
        headerData.put(tag);
        headerData.putLong(0L);
        headerData.putLong(this.startOffset);
        headerData.putLong(this.firstLogIndex);
        headerData.flip();
        return headerData.array();
    }

    public boolean decode(final byte[] headerData) {
        if (headerData == null || headerData.length < _HEADER_BYTE_SIZE) {
            return false;
        }
        final ByteBuffer byteBuffer = ByteBuffer.wrap(headerData);
        if (byteBuffer.get() != _MAGIC) {
            return false;
        }
        if (byteBuffer.get() != _MAGIC) {
            return false;
        }
        byteBuffer.getLong();//reserved
        this.startOffset = byteBuffer.getLong();
        this.firstLogIndex = byteBuffer.getLong();
        return true;
    }

    public boolean isBlank() {
        return this.tag == TAG_BLANK;
    }

    /**
     *
     * @return
     */
    public boolean isConsecutive() {
        return (this.tag & TAG_CONSECUTIVE) != 0;
    }

    public void setConsecutive() {
        this.tag |= TAG_CONSECUTIVE;
    }

    public boolean isAvailable() {
        return (this.tag & TAG_AVAILABLE) != 0;
    }

    public void setAvailable() {
        this.tag |= TAG_AVAILABLE;
    }

    public static int getBytesSize() {
        return _HEADER_BYTE_SIZE;
    }

    public void rest() {
        this.tag = TAG_BLANK;
    }

}
