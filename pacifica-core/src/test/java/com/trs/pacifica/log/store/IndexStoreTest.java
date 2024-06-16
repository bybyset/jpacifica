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

import com.trs.pacifica.log.store.file.FileHeader;
import com.trs.pacifica.log.store.file.IndexEntry;
import com.trs.pacifica.log.store.file.IndexFile;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.test.BaseStorageTest;
import com.trs.pacifica.util.Tuple2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class IndexStoreTest extends BaseStorageTest {

    IndexStore indexStore;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        File dir = new File(this.path);
        indexStore = new IndexStore(dir.toPath());
        indexStore.load();
    }

    @AfterEach
    @Override
    public void teardown() throws Exception {
        this.indexStore.close();
        super.teardown();
    }

    @Test
    void testAppendLogIndex() throws IOException {
        final LogId logId1 = new LogId(1001, 1);
        Tuple2<Integer, Long> result = this.indexStore.appendLogIndex(logId1, 20);
        Assertions.assertEquals(FileHeader.getBytesSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize(), result.getSecond());

        final LogId logId2 = new LogId(1002, 1);
        result = this.indexStore.appendLogIndex(logId2, 30);
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize() * 2, result.getSecond());

        final LogId logId3 = new LogId(1003, 1);
        result =  this.indexStore.appendLogIndex(logId3, 40);
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize() * 2, result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize() * 3, result.getSecond());


        Assertions.assertEquals(1001, indexStore.getFirstLogIndex());
        Assertions.assertEquals(1003, indexStore.getLastLogIndex());

    }

    @Test
    void testAppendLogIndex2() throws IOException {
        final LogId logId1 = new LogId(1001, 1);
        IndexEntry indexEntry1 = new IndexEntry(logId1, 20);
        Tuple2<Integer, Long> result = this.indexStore.appendLogIndex(indexEntry1);
        Assertions.assertEquals(FileHeader.getBytesSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize(), result.getSecond());

        final LogId logId2 = new LogId(1002, 1);
        IndexEntry indexEntry2 = new IndexEntry(logId2, 30);
        result = this.indexStore.appendLogIndex(indexEntry2);
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize(), result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize() * 2, result.getSecond());

        final LogId logId3 = new LogId(1003, 1);
        IndexEntry indexEntry3 = new IndexEntry(logId3, 40);
        result =  this.indexStore.appendLogIndex(indexEntry3);
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize() * 2, result.getFirst());
        Assertions.assertEquals(FileHeader.getBytesSize() + IndexFile.getWriteByteSize() * 3, result.getSecond());


        final LogId logId5 = new LogId(1005, 1);
        IndexEntry indexEntry5 = new IndexEntry(logId5, 60);
        Throwable t = null;
        try {
            this.indexStore.appendLogIndex(indexEntry5);
        } catch (Exception e) {
            t = e;
        }
        Assertions.assertNotNull(t);

        Assertions.assertEquals(1001, indexStore.getFirstLogIndex());
        Assertions.assertEquals(1003, indexStore.getLastLogIndex());
    }

    @Test
    public void testLookupPositionAt() throws IOException {
        final LogId logId1 = new LogId(1001, 1);
        int position1 = 20;
        IndexEntry indexEntry1 = new IndexEntry(logId1, position1);
        this.indexStore.appendLogIndex(indexEntry1);

        final LogId logId2 = new LogId(1002, 1);
        int position2 = 20;
        IndexEntry indexEntry2 = new IndexEntry(logId2, position2);
         this.indexStore.appendLogIndex(indexEntry2);

        int result1 = this.indexStore.lookupPositionAt(1001);
        Assertions.assertEquals(position1, result1);
        int result2 = this.indexStore.lookupPositionAt(1002);
        Assertions.assertEquals(position2, result2);
    }

}
