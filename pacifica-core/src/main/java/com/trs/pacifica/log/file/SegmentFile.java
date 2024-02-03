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

import java.io.IOException;
import java.nio.ByteBuffer;

public class SegmentFile extends AbstractFile {

    /**
     * Magic bytes for data buffer.
     */
    private static final byte[] _SEGMENT_MAGIC_BYTES = new byte[]{(byte) 0x57, (byte) 0x8A};

    // 4 Bytes for written data length
    static final int _SEGMENT_DATA_LENGTH_SIZE = 4;


    public SegmentFile(String filePath, int fileSize, long firstLogIndex, long startOffset) {
        super(filePath, fileSize, firstLogIndex, startOffset);
    }

    public int appendSegmentData(final long logIndex, final byte[] data) throws IOException {
        final byte[] segmentEntry = encodeData(data);
        return doAppendData(logIndex, segmentEntry);
    }




    private byte[] encodeData(final byte[] data) {
        //TODO optimization
        ByteBuffer buffer = ByteBuffer.allocate(getWriteByteSize(data));
        buffer.put(_SEGMENT_MAGIC_BYTES);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();
        return buffer.array();
    }


    public static int getWriteByteSize(final byte[] data) {
        return _SEGMENT_DATA_LENGTH_SIZE + _SEGMENT_MAGIC_BYTES.length + data.length;
    }

}
