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
 * --------------+----------+--------------+-----------------+----------+
 * | magic byte | file size | start offset | first log index | reserved |
 * |[0x20][0x20]| [4 bytes] | [ 8   bytes ]| [    8  bytes  ]| [8 bytes]|
 * -------------+-------- --+-------------+------------------+----------+
 * </pre>
 */
public class FileHeader {

    private static final byte MAGIC = 0X20;

    static final int HEADER_BYTES_SIZE = 30;

    private volatile long firstLogIndex = Long.MIN_VALUE;

    private volatile long lastLogIndex = Long.MIN_VALUE;

    private long startOffset;

    private int fileSize;


    public long getFirstLogIndex() {
        return firstLogIndex;
    }

    public void setFirstLogIndex(long firstLogIndex) {
        this.firstLogIndex = firstLogIndex;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }


    public ByteBuffer encode() {
        final ByteBuffer headerData = ByteBuffer.allocate(HEADER_BYTES_SIZE);
        headerData.put(MAGIC);
        headerData.put(MAGIC);
        headerData.putInt(this.fileSize);
        headerData.putLong(this.startOffset);
        headerData.putLong(this.firstLogIndex);
        headerData.putLong(0L);
        headerData.flip();
        return headerData;
    }

    public boolean decode(final ByteBuffer headerData) {
        if (headerData == null || headerData.remaining() < HEADER_BYTES_SIZE) {
            return false;
        }
        if (headerData.get() != MAGIC) {
            return false;
        }
        if (headerData.get() != MAGIC) {
            return false;
        }
        this.fileSize = headerData.getInt();
        this.startOffset = headerData.getLong();
        this.firstLogIndex = headerData.getLong();
        headerData.getLong();
        return true;
    }
}
