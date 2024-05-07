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

import com.trs.pacifica.log.dir.Directory;
import com.trs.pacifica.util.BitUtil;
import com.trs.pacifica.util.io.ByteDataBuffer;
import com.trs.pacifica.util.io.DataBuffer;
import com.trs.pacifica.util.io.DataInput;
import com.trs.pacifica.util.io.LinkedDataBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SegmentFile extends AbstractFile {

    public SegmentFile(Directory parentDir, String filename) throws IOException {
        super(parentDir, filename);
    }

    @Override
    protected void loadBody() throws IOException {

    }


    /**
     *
     * @param logIndex
     * @param segmentBlock segment block
     * @return number of bytes written
     * @throws IOException
     */
    public int appendLogEntry(final long logIndex, final Block segmentBlock) throws IOException {
        Objects.requireNonNull(segmentBlock, "segmentBlock");
        if (logIndex <= 0) {
            throw new IllegalArgumentException("illegal params logIndex=" + logIndex);
        }
        return doAppendData(logIndex, segmentBlock.encode());
    }


    /**
     *
     * @param segmentBlock segment block
     * @return number of bytes written
     * @throws IOException
     */
    public int appendLogEntry(final Block segmentBlock) throws IOException {
        Objects.requireNonNull(segmentBlock, "segmentBlock");
        return doAppendData(segmentBlock.encode());
    }



    public Block lookupBlock(final int position) throws IOException {
        final byte[] blockHeader = new byte[Block.HEADER_SIZE];
        this.readBytes(blockHeader, position);
        final int dataLen = Block.decodeDataLen(blockHeader);
        final DataBuffer logEntryData;
        if (dataLen > 0) {
            logEntryData = this.readDataBuffer(position + Block.HEADER_SIZE, dataLen);
        } else {
            logEntryData = Block.EMPTY_DATA_BUFFER;
        }
        return Block.decode(blockHeader, logEntryData);
    }

    public static int getWriteByteSize(final int logDataByteSize) {
        return Block.HEADER_SIZE + logDataByteSize;
    }

    public static Block wrapBlock(final DataBuffer logEntryData) {
        return new Block(logEntryData);
    }

}
