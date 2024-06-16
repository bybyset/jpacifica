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
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.io.ByteDataBuffer;

import java.io.IOException;

import static com.trs.pacifica.log.store.file.IndexEntry.*;

public class IndexFile extends AbstractFile {

    static final int _INDEX_ENTRY_BYTE_SIZE = IndexEntryHeader.byteSize() + IndexEntry.byteSize();

    public IndexFile(Directory parentDir, String filename) throws IOException {
        super(parentDir, filename);
    }

    @Override
    protected CheckEntryResult checkEntry(Input fileReader) throws IOException {
        final byte magic = fileReader.readByte();
        if (isFileEnd(magic) || IndexEntryHeader.HEADER_MAGIC != magic) {
            return CheckEntryResult.fileEnd();
        }
        return CheckEntryResult.success(1, _INDEX_ENTRY_BYTE_SIZE);
    }

    @Override
    protected int lookupPositionFromHead(long logIndex) {
        final long firstLogIndex = this.header.getFirstLogIndex();
        return FileHeader.getBytesSize() + getWriteByteSize() * (int)(logIndex - firstLogIndex);
    }

    /**
     * append index
     * @param logId
     * @param logPosition
     * @return the starting position before writing to the file
     * @throws IOException
     */
    public int appendIndexData(final LogId logId, final int logPosition) throws IOException {
        final long logIndex = logId.getIndex();
        final byte[] appendData = new byte[_INDEX_ENTRY_BYTE_SIZE];
        encodeIndexEntryHeader(appendData);
        byte[] indexEntry = encodeIndexEntry(logId, logPosition);
        System.arraycopy(indexEntry, 0, appendData, IndexEntryHeader.byteSize(), indexEntry.length);
        int startWritePosition = this.getWrotePosition();
        int writeBytes = doAppendData(logIndex, new ByteDataBuffer(appendData));
        assert writeBytes == appendData.length;
        return startWritePosition;
    }

    /**
     * lookup position of the log in segment file
     *
     * @param logIndex
     * @return
     */
    public IndexEntry lookupIndexEntry(final long logIndex) throws IOException {
        //calculating position
        int position = calculatingPosition(logIndex);
        final long fileSize = this.getFileSize();
        if (position < fileSize) {
            //read bytes
            final byte[] bytes = new byte[_INDEX_ENTRY_BYTE_SIZE];
            final int len = this.readBytes(bytes, position);
            if (len != -1) {
                assert len == _INDEX_ENTRY_BYTE_SIZE;
                final IndexEntryHeader indexEntryHeader = IndexEntryHeader.from(bytes);
                final IndexEntryCodec indexEntryCodec = INDEX_ENTRY_CODEC_FACTORY.getIndexEntryCodec(indexEntryHeader);
                final byte[] indexEntryBytes = new byte[bytes.length - IndexEntryHeader.byteSize()];
                System.arraycopy(bytes, IndexEntryHeader.byteSize(), indexEntryBytes, 0, indexEntryBytes.length);
                return indexEntryCodec.decode(indexEntryBytes, IndexEntryHeader.byteSize());
            }
        }
        return null;
    }

    private int calculatingPosition(final long logIndex) {
        // header size + offset * size_per_entry
        return FileHeader.getBytesSize() + toRelativeOffset(logIndex) * getWriteByteSize();
    }


    private static void encodeIndexEntryHeader(final byte[] container) {
        container[0] = IndexEntryHeader.HEADER_MAGIC;
        container[1] = IndexEntryHeader.HEADER_VERSION;
    }

    private byte[] encodeIndexEntry(final LogId logId, final int position) {
        final IndexEntry indexEntry = new IndexEntry(logId, position);
        return INDEX_ENTRY_CODEC.encode(indexEntry);
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


}
