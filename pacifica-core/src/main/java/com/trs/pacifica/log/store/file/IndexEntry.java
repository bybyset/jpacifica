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

import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.BitUtil;

public class IndexEntry {

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


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof IndexEntry)) {
            return false;
        }
        if (!this.logId.equals(((IndexEntry) obj).logId)) {
            return false;
        }
        return this.position == ((IndexEntry) obj).position;
    }

    public static int byteSize() {
        return Long.BYTES + Long.BYTES + Integer.BYTES;
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

    public static class IndexEntryCodecV1 implements IndexEntryCodec {

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
            return new IndexEntry(new LogId(logIndex, logTerm), position);
        }
    }
}
