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

package com.trs.pacifica.log.store;

import com.trs.pacifica.log.store.file.Block;
import com.trs.pacifica.log.store.file.FileHeader;
import com.trs.pacifica.test.BaseStorageTest;
import com.trs.pacifica.util.Tuple2;
import com.trs.pacifica.util.io.ByteDataBuffer;
import com.trs.pacifica.util.io.DataBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class SegmentStoreTest extends BaseStorageTest {

    SegmentStore segmentStore;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        File dir = new File(this.path);
        segmentStore = new SegmentStore(dir.toPath(), 10 * 1024);
        segmentStore.load();
    }

    @AfterEach
    @Override
    public void shutdown() throws Exception {
        this.segmentStore.close();
        super.shutdown();
    }

    @Test
    void testAppendLogData() throws IOException {
        final long logIndex1 = 1001;
        byte[] logData1 = "test_append_log_data1".getBytes();
        Tuple2<Integer, Long> result = this.segmentStore.appendLogData(logIndex1, new ByteDataBuffer(logData1));
        Assertions.assertEquals(FileHeader.getBytesSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + Block.HEADER_SIZE, result.getSecond());

        final long logIndex2 = 1002;
        byte[] logData2 = "test_append_log_data2".getBytes();
        result = this.segmentStore.appendLogData(logIndex2, new ByteDataBuffer(logData2));
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + +Block.HEADER_SIZE, result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + logData2.length + Block.HEADER_SIZE * 2, result.getSecond());


        final long logIndex3 = 1003;
        byte[] logData3 = "test_append_log_data3".getBytes();
        result = this.segmentStore.appendLogData(logIndex3, new ByteDataBuffer(logData3));
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + logData2.length + Block.HEADER_SIZE * 2, result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + logData2.length + logData3.length + Block.HEADER_SIZE * 3, result.getSecond());


        final long logIndex5 = 1005;
        byte[] logData5 = "test_append_log_data5".getBytes();
        Throwable t = null;
        try {
            result = this.segmentStore.appendLogData(logIndex5, new ByteDataBuffer(logData5));
        } catch (Exception e) {
            t = e;
        }
        Assertions.assertNotNull(t);


        Assertions.assertEquals(1001, segmentStore.getFirstLogIndex());
        Assertions.assertEquals(1003, segmentStore.getLastLogIndex());

    }

    @Test
    void testAppendLogDataOneLogEntryTowFile() throws IOException {
        final long logIndex1 = 1001;
        byte[] logData1 = mockBytes(50);
        Tuple2<Integer, Long> result = this.segmentStore.appendLogData(logIndex1, new ByteDataBuffer(logData1));
        Assertions.assertEquals(FileHeader.getBytesSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + Block.HEADER_SIZE, result.getSecond());

        final long logIndex2 = 1002;
        byte[] logData2 = mockBytes(11 * 1024);
        result = this.segmentStore.appendLogData(logIndex2, new ByteDataBuffer(logData2));
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + +Block.HEADER_SIZE, result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() * 2 + logData1.length + logData2.length + Block.HEADER_SIZE * 3, result.getSecond());


        final long logIndex3 = 1003;
        byte[] logData3 = mockBytes(5 * 1024);
        result = this.segmentStore.appendLogData(logIndex3, new ByteDataBuffer(logData3));
        Assertions.assertEquals(FileHeader.getBytesSize() * 2 + logData1.length + logData2.length + Block.HEADER_SIZE * 3 - this.segmentStore.getFileSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() * 2 + logData1.length + logData2.length + logData3.length + Block.HEADER_SIZE * 4, result.getSecond());

        Assertions.assertEquals(1001, segmentStore.getFirstLogIndex());
        Assertions.assertEquals(1003, segmentStore.getLastLogIndex());

    }

    @Test
    void testAppendLogDataOneLogEntryThreeFile() throws IOException {
        final long logIndex1 = 1001;
        byte[] logData1 = mockBytes(50);
        Tuple2<Integer, Long> result = this.segmentStore.appendLogData(logIndex1, new ByteDataBuffer(logData1));
        Assertions.assertEquals(FileHeader.getBytesSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + Block.HEADER_SIZE, result.getSecond());

        final long logIndex2 = 1002;
        byte[] logData2 = mockBytes(21 * 1024);
        result = this.segmentStore.appendLogData(logIndex2, new ByteDataBuffer(logData2));
        Assertions.assertEquals(FileHeader.getBytesSize() + logData1.length + +Block.HEADER_SIZE, result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() * 3 + logData1.length + logData2.length + Block.HEADER_SIZE * 4, result.getSecond());


        final long logIndex3 = 1003;
        byte[] logData3 = mockBytes(1 * 1024);
        result = this.segmentStore.appendLogData(logIndex3, new ByteDataBuffer(logData3));
        Assertions.assertEquals(FileHeader.getBytesSize() * 3 + logData1.length + logData2.length + Block.HEADER_SIZE * 4 - this.segmentStore.getFileSize() * 2, result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() * 3 + logData1.length + logData2.length + logData3.length + Block.HEADER_SIZE * 5, result.getSecond());

        Assertions.assertEquals(1001, segmentStore.getFirstLogIndex());
        Assertions.assertEquals(1003, segmentStore.getLastLogIndex());

    }

    @Test
    public void testLookupLogEntry() throws IOException {
        final long logIndex1 = 1001;
        byte[] logData1 = "test_append_log_data1".getBytes();
        Tuple2<Integer, Long> result1 = this.segmentStore.appendLogData(logIndex1, new ByteDataBuffer(logData1));

        final long logIndex2 = 1002;
        byte[] logData2 = "test_append_log_data2".getBytes();
        Tuple2<Integer, Long> result2 = this.segmentStore.appendLogData(logIndex2, new ByteDataBuffer(logData2));


        final long logIndex3 = 1003;
        byte[] logData3 = "test_append_log_data3".getBytes();
        Tuple2<Integer, Long> result3 = this.segmentStore.appendLogData(logIndex3, new ByteDataBuffer(logData3));


        final DataBuffer logEntry2 = this.segmentStore.lookupLogEntry(logIndex2, result2.getFirst());
        final byte[] logEntry2Bytes = logEntry2.readRemain();
        Assertions.assertArrayEquals(logData2, logEntry2Bytes);

    }

    @Test
    public void testLookupLogEntryOneLogEntryTowFile() throws IOException {
        final long logIndex1 = 1001;
        byte[] logData1 = mockBytes(50);
        Tuple2<Integer, Long> result1 = this.segmentStore.appendLogData(logIndex1, new ByteDataBuffer(logData1));

        final long logIndex2 = 1002;
        byte[] logData2 = mockBytes(11 * 1024);
        Tuple2<Integer, Long> result2 = this.segmentStore.appendLogData(logIndex2, new ByteDataBuffer(logData2));


        final long logIndex3 = 1003;
        byte[] logData3 = mockBytes(5 * 1024);
        Tuple2<Integer, Long> result3 = this.segmentStore.appendLogData(logIndex3, new ByteDataBuffer(logData3));

        final DataBuffer logEntry2 = this.segmentStore.lookupLogEntry(logIndex2, result2.getFirst());
        final byte[] logEntry2Bytes = logEntry2.readRemain();
        Assertions.assertArrayEquals(logData2, logEntry2Bytes);

    }

    @Test
    public void testLookupLogEntryOneLogEntryThreeFile() throws IOException {
        final long logIndex1 = 1001;
        byte[] logData1 = mockBytes(50);
        Tuple2<Integer, Long> result1 = this.segmentStore.appendLogData(logIndex1, new ByteDataBuffer(logData1));

        final long logIndex2 = 1002;
        byte[] logData2 = mockBytes(21 * 1024);
        Tuple2<Integer, Long> result2 = this.segmentStore.appendLogData(logIndex2, new ByteDataBuffer(logData2));


        final long logIndex3 = 1003;
        byte[] logData3 = mockBytes(1 * 1024);
        Tuple2<Integer, Long> result3 = this.segmentStore.appendLogData(logIndex3, new ByteDataBuffer(logData3));

        final DataBuffer logEntry2 = this.segmentStore.lookupLogEntry(logIndex2, result2.getFirst());
        final byte[] logEntry2Bytes = logEntry2.readRemain();
        Assertions.assertArrayEquals(logData2, logEntry2Bytes);

    }



    static byte[] mockBytes(int size, byte fill) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, fill);
        return bytes;
    }

    static byte[] mockBytes(int size) {
        byte[] bytes = new byte[size];
        byte fill = (byte) new Random().nextInt(255);
        Arrays.fill(bytes, fill);
        return bytes;
    }

}
