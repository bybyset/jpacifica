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
import com.trs.pacifica.log.store.IndexStore;
import com.trs.pacifica.model.LogId;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IndexFile extends AbstractFile {

    /**
     * Magic bytes for data buffer.
     */
    private static final byte[] _INDEX_MAGIC_BYTES = new byte[]{(byte) 0x27, (byte) 0x34};

    static final int _INDEX_ENTRY_BYTE_SIZE = _INDEX_MAGIC_BYTES.length + Integer.BYTES + Integer.BYTES;

    public IndexFile(Directory parentDir, String filename) throws IOException {
        super(parentDir, filename);
    }


    public int appendIndexData(final LogId logId, final int logPosition) throws IOException {
        final long logIndex = logId.getIndex();
        final byte[] indexEntry = encodeData(toRelativeOffset(logIndex), logPosition);
        return doAppendData(logIndex, indexEntry);
    }

    /**
     * lookup position of the log in segment file
     *
     * @param logIndex
     * @return
     */
    public IndexEntry lookupIndexEntry(final long logIndex) {
        //calculating position
        int position = calculatingPosition(logIndex);
        //read bytes

        return null;
    }

    private int calculatingPosition(final long logIndex) {
        // header size + offset * size_per_entry
        return FileHeader.getBytesSize() + toRelativeOffset(logIndex) * getWriteByteSize();
    }


    private byte[] encodeData(final int offset, final int position) {
        final ByteBuffer buffer = ByteBuffer.allocate(getWriteByteSize());
        // Magics
        buffer.put(_INDEX_MAGIC_BYTES);
        // offset from first log index
        // TODO Do we need it?
        buffer.putInt(offset);
        // start position of the log entry in segment file
        buffer.putInt(position);
        buffer.flip();
        return buffer.array();
    }

    /**
     * Return the relative offset
     */
    private int toRelativeOffset(final long logIndex) {
        if (this.header.isBlank()) {
            return 0;
        } else {
            return (int) (logIndex - this.header.getFirstLogIndex());
        }
    }

    public static int getWriteByteSize() {
        return _INDEX_ENTRY_BYTE_SIZE;
    }


    public static class IndexEntry {

        private final LogId logId;

        private final int position;

        public IndexEntry(LogId logId, int position) {
            this.logId = logId;
            this.position = position;
        }


        public LogId getLogId() {
            return logId;
        }

        public int getPosition() {
            return position;
        }
    }

    public static class IndexEntryHeader {

    }

    public static interface IndexEntryCodec {

        byte[] encode();

        IndexEntry decode(ByteBuffer byteBuffer);

    }



}
