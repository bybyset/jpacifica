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

package com.trs.pacifica.log.store.file;

import com.trs.pacifica.log.dir.Directory;
import com.trs.pacifica.log.io.Input;
import com.trs.pacifica.util.io.DataBuffer;

import java.io.IOException;
import java.util.Objects;

public class SegmentFile extends AbstractFile {

    public SegmentFile(Directory parentDir, String filename) throws IOException {
        super(parentDir, filename);
    }

    @Override
    protected CheckEntryResult checkEntry(Input fileReader) throws IOException {
        final byte[] blockHeader = new byte[Block.HEADER_SIZE];
        fileReader.readBytes(blockHeader);
        if (isFileEnd(blockHeader[0]) || Block.SEGMENT_BLOCK_MAGIC != blockHeader[0]) {
            return CheckEntryResult.fileEnd();
        }
        Block block = Block.decode(blockHeader, null);
        final int dataByteSize = block.getDataLen();
        final int entryNum;
        if (block.isFirstBlock()) {
            entryNum = 1;
        } else {
            entryNum = 0;
        }
        return CheckEntryResult.success(entryNum, dataByteSize + Block.HEADER_SIZE);
    }

    @Override
    protected int lookupPositionFromHead(long logIndex) {
        return 0;
    }


    /**
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
     * @param segmentBlock segment block
     * @return number of bytes written
     * @throws IOException
     */
    public int appendLogEntry(final Block segmentBlock) throws IOException {
        Objects.requireNonNull(segmentBlock, "segmentBlock");
        return doAppendData(segmentBlock.encode());
    }


    /**
     * Reads a block starting at the specified position
     *
     * @param position position in the file
     * @return null if the position is greater than file size or is end of file or not found
     * @throws IOException
     */
    public Block lookupBlock(final int position) throws IOException {
        if (position >= fileSize) {
            return null;
        }
        final byte[] blockHeader = new byte[Block.HEADER_SIZE];
        this.readBytes(blockHeader, position);
        if (isFileEnd(blockHeader)) {
            return null;
        }
        final Block block = Block.decode(blockHeader);
        if (block != null) {
            final int dataLen = block.getDataLen();
            final DataBuffer logEntryData;
            if (block.getDataLen() > 0) {
                logEntryData = this.readDataBuffer(position + Block.HEADER_SIZE, dataLen);
            } else {
                logEntryData = Block.EMPTY_DATA_BUFFER;
            }
            return Block.decode(blockHeader, logEntryData);
        }
        return null;
    }

    public static int getWriteByteSize(final int logDataByteSize) {
        return Block.HEADER_SIZE + logDataByteSize;
    }

    public static Block wrapBlock(final DataBuffer logEntryData) {
        return new Block(logEntryData);
    }

}
