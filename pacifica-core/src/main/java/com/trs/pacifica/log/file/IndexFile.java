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
import com.trs.pacifica.log.io.Input;
import com.trs.pacifica.log.store.IndexStore;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.BitUtil;
import com.trs.pacifica.util.io.ByteDataBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IndexFile extends AbstractFile {

    static final int _INDEX_ENTRY_BYTE_SIZE = IndexEntryHeader.byteSize() + IndexEntry.byteSize();

    public IndexFile(Directory parentDir, String filename) throws IOException {
        super(parentDir, filename);
    }

    @Override
    protected void loadBody() throws IOException {
        final int fileSize = this.getFileSize();
        int currentPos = FileHeader.getBytesSize();
        final byte[] readBuffer = new byte[_INDEX_ENTRY_BYTE_SIZE];
        try (final Input input = this.parentDir.openInOutput(this.filename);){
            do {
                input.seek(currentPos);
                final byte magic = input.readByte();
                if (magic != IndexEntryHeader.HEADER_MAGIC) {
                    break;
                }
                this.lastLogIndex++;
                currentPos += _INDEX_ENTRY_BYTE_SIZE;
            } while (currentPos < fileSize);
        }
    }


    public int appendIndexData(final LogId logId, final int logPosition) throws IOException {
        final long logIndex = logId.getIndex();
        final byte[] appendData = new byte[_INDEX_ENTRY_BYTE_SIZE];
        encodeIndexEntryHeader(appendData);
        byte[] indexEntry = encodeIndexEntry(logId, logPosition);
        System.arraycopy(indexEntry, 0, appendData, IndexEntryHeader.byteSize(), indexEntry.length);
        return doAppendData(logIndex, new ByteDataBuffer(appendData));
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
                return indexEntryCodec.decode(bytes, IndexEntryHeader.byteSize());
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


        static int byteSize() {
            return Long.BYTES + Long.BYTES + Integer.BYTES;
        }

    }

    public static class IndexEntryHeader {

        static final byte HEADER_MAGIC = 0x27;

        static final byte HEADER_VERSION = 0x01;

        private final byte magic;

        private final byte version;


        public IndexEntryHeader(byte magic, byte version) {
            this.magic = magic;
            this.version = version;
        }

        public byte getMagic() {
            return magic;
        }

        public byte getVersion() {
            return version;
        }


        static IndexEntryHeader from(byte[] bytes) {
            assert bytes.length > 2;
            return new IndexEntryHeader(bytes[0], bytes[1]);
        }

        static int byteSize() {
            return 2;
        }
    }

    static final IndexEntryCodec INDEX_ENTRY_CODEC = new IndexEntryCodecV1();

    static final IndexEntryCodecFactory INDEX_ENTRY_CODEC_FACTORY = new IndexEntryCodecFactory() {

        @Override
        public IndexEntryCodec getIndexEntryCodec(IndexEntryHeader indexEntryHeader) {
            return INDEX_ENTRY_CODEC;
        }
    };

    public static interface IndexEntryCodecFactory {

        IndexEntryCodec getIndexEntryCodec(IndexEntryHeader indexEntryHeader);

    }

    public static interface IndexEntryCodec {

        byte[] encode(IndexEntry indexEntry);

        IndexEntry decode(final byte[] bytes, final int offset);

        default IndexEntry decode(final byte[] bytes) {
            return decode(bytes, 0);
        }

    }

    public static class IndexEntryCodecV1 implements IndexEntryCodec{

        @Override
        public byte[] encode(IndexEntry indexEntry) {
            byte[] encodeBytes = new byte[IndexEntry.byteSize()];
            BitUtil.putLong(encodeBytes, 0, indexEntry.logId.getIndex());
            BitUtil.putLong(encodeBytes, 8, indexEntry.logId.getTerm());
            BitUtil.putInt(encodeBytes, 16, indexEntry.getPosition());
            return encodeBytes;
        }

        @Override
        public IndexEntry decode(final byte[] bytes, final int offset) {
            final long logIndex = BitUtil.getLong(bytes, 0);
            final long logTerm = BitUtil.getLong(bytes, 8);
            final int position = BitUtil.getInt(bytes, 16);
            return new IndexEntry(new LogId(logIndex,logTerm), position);
        }
    }



}
