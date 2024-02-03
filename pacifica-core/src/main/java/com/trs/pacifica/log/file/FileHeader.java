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

import java.nio.ByteBuffer;

/**
 * HEADER:
 * <pre>
 * --------------+--------------+-----------------+----------+
 * | magic byte | start offset | first log index | reserved |
 * |[0x20][0x20]| [ 8   bytes ]| [    8  bytes  ]| [8 bytes]|
 * -------------+-------------+------------------+----------+
 * </pre>
 */
public class FileHeader {

    static final long _BLANK_TAG = -1L;

    static final int _HEADER_BYTE_SIZE = 26;

    private static final byte _MAGIC = 0X20;

    private long firstLogIndex = _BLANK_TAG;

    private long startOffset = _BLANK_TAG;

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


    public ByteBuffer encode() {
        final ByteBuffer headerData = ByteBuffer.allocate(_HEADER_BYTE_SIZE);
        headerData.put(_MAGIC);
        headerData.put(_MAGIC);
        headerData.putLong(this.startOffset);
        headerData.putLong(this.firstLogIndex);
        headerData.putLong(0L);
        headerData.flip();
        return headerData;
    }

    public boolean decode(final ByteBuffer headerData) {
        if (headerData == null || headerData.remaining() < _HEADER_BYTE_SIZE) {
            return false;
        }
        if (headerData.get() != _MAGIC) {
            return false;
        }
        if (headerData.get() != _MAGIC) {
            return false;
        }
        this.startOffset = headerData.getLong();
        this.firstLogIndex = headerData.getLong();
        headerData.getLong();
        return true;
    }

    public boolean isBlank() {
        return this.firstLogIndex < 0;
    }

    public int getBytesSize() {
        return _HEADER_BYTE_SIZE;
    }

}
