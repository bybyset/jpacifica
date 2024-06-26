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

package com.trs.pacifica.snapshot.storage;

import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

public class DefaultSnapshotStorageTest extends BaseStorageTest {


    private DefaultSnapshotStorage snapshotStorage;


    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        this.snapshotStorage = new DefaultSnapshotStorage(this.path);
        this.snapshotStorage = Mockito.spy(this.snapshotStorage);
    }

    @AfterEach
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }


    @Test
    public void testLoadEmpty() throws IOException {
        //
        this.snapshotStorage.load();
        Assertions.assertEquals(0, this.snapshotStorage.getLastSnapshotIndex());
        Assertions.assertEquals(0, this.snapshotStorage.getRef(this.snapshotStorage.getSnapshotName(0)));
    }

    @Test
    public void testLoadNotEmpty() throws IOException {
        //
        long snapshotLogIndex1 = 1001;
        String snapshotPath = this.snapshotStorage.getSnapshotPath(snapshotLogIndex1);
        File snapshotDir = new File(snapshotPath);
        snapshotDir.mkdir();

        long snapshotLogIndex3 = 1003;
        String snapshotPath3 = this.snapshotStorage.getSnapshotPath(snapshotLogIndex3);
        File snapshotDir3 = new File(snapshotPath3);
        snapshotDir3.mkdir();
        this.snapshotStorage.load();
        Assertions.assertEquals(snapshotLogIndex3, this.snapshotStorage.getLastSnapshotIndex());
        Assertions.assertEquals(1, this.snapshotStorage.getRef(this.snapshotStorage.getSnapshotName(snapshotLogIndex3)));
    }
    @Test
    public void testOpenSnapshotWriter() throws IOException {
        this.snapshotStorage.load();
        LogId snapshotLogId = new LogId(1004, 1);
        String tempSnapshotName = this.snapshotStorage.getTempSnapshotName();
        String tempWriterPath = this.snapshotStorage.getSnapshotPath(tempSnapshotName);
        SnapshotWriter snapshotWriter =  this.snapshotStorage.openSnapshotWriter(snapshotLogId);
        Assertions.assertTrue(snapshotWriter instanceof  DefaultSnapshotWriter);
        Assertions.assertEquals(1, this.snapshotStorage.getRef(tempSnapshotName));
        Assertions.assertEquals(tempWriterPath, snapshotWriter.getDirectory());
    }

    @Test
    public void testOpenSnapshotReader() throws IOException {
        mockSnapshotDir();
        this.snapshotStorage.load();

        long snapshotLogIndex = 1003;
        String snapshotName = this.snapshotStorage.getSnapshotName(snapshotLogIndex);
        SnapshotReader snapshotReader = this.snapshotStorage.openSnapshotReader();
        Assertions.assertTrue(snapshotReader instanceof DefaultSnapshotReader);
        Assertions.assertEquals(snapshotLogIndex, this.snapshotStorage.getLastSnapshotIndex());
        Assertions.assertEquals(2, this.snapshotStorage.getRef(snapshotName));

    }



    private void mockSnapshotDir() throws IOException {
        long snapshotLogIndex1 = 1001;
        String snapshotPath = this.snapshotStorage.getSnapshotPath(snapshotLogIndex1);
        File snapshotDir = new File(snapshotPath);
        snapshotDir.mkdir();

        long snapshotLogIndex3 = 1003;
        String snapshotPath3 = this.snapshotStorage.getSnapshotPath(snapshotLogIndex3);
        File snapshotDir3 = new File(snapshotPath3);
        snapshotDir3.mkdir();
        LogId logId3 = new LogId(snapshotLogIndex3, 1);
        DefaultSnapshotMeta snapshotMeta = new DefaultSnapshotMeta(logId3);

        String snapshotMetaFilePath = DefaultSnapshotMeta.getSnapshotMetaFilePath(snapshotPath3);
        DefaultSnapshotMeta.saveToFile(snapshotMeta, snapshotMetaFilePath, true);

    }


}
