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

import com.trs.pacifica.util.BitUtil;
import com.trs.pacifica.util.io.ByteDataBuffer;
import com.trs.pacifica.util.io.DataBuffer;
import com.trs.pacifica.util.io.LinkedDataBuffer;

public class Block {
    static final DataBuffer EMPTY_DATA_BUFFER = new ByteDataBuffer(new byte[0]);
    public static final int HEADER_SIZE = 6;//6 bytes = magic(1 byte) + tag(1 byte) + data_length(4 byte)
    static final byte SEGMENT_BLOCK_MAGIC = 0x08;
    static final byte TAG_HAS_NEXT = 1;
    private byte magic = SEGMENT_BLOCK_MAGIC;
    private byte tag = 0x00;
    private int dataLen;
    private DataBuffer logEntryData;

    public Block(DataBuffer logEntryData) {
        this.logEntryData = logEntryData;
        this.dataLen = logEntryData.remaining();
    }

    public Block() {
        this(EMPTY_DATA_BUFFER);
    }

    Block(final byte magic, final byte tag, final int dataLen, final DataBuffer logEntryData) {
        this.magic = magic;
        this.tag = tag;
        this.dataLen = dataLen;
        this.logEntryData = logEntryData;
    }

    public boolean hasNextBlock() {
        return (this.tag & TAG_HAS_NEXT) != 0;
    }

    public void setHasNextBlock() {
        this.tag |= TAG_HAS_NEXT;
    }

    public int getDataLen() {
        return dataLen;
    }

    public void setLogEntryData(DataBuffer logEntryData) {
        this.logEntryData = logEntryData;
    }

    public DataBuffer getLogEntryData() {
        return logEntryData;
    }

    public DataBuffer encode() {
        final byte[] header = new byte[HEADER_SIZE];
        header[0] = this.magic;
        header[1] = this.tag;
        BitUtil.putInt(header, 2, this.dataLen);
        final DataBuffer headerData = new ByteDataBuffer(header);
        return new LinkedDataBuffer(headerData, logEntryData);
    }

    public static Block decode(final byte[] header, final DataBuffer logEntryData) {
        if (header == null || header.length != HEADER_SIZE) {
            throw new IllegalArgumentException("illegal header of block");
        }
        final byte magic = header[0];
        final byte tag = header[1];
        final int dataLen = BitUtil.getInt(header, 2);
        return new Block(magic, tag, dataLen, logEntryData);
    }

    public static int decodeDataLen(final byte[] header) {
        if (header == null || header.length != HEADER_SIZE) {
            throw new IllegalArgumentException("illegal header of block");
        }
        return BitUtil.getInt(header, 2);
    }
}
