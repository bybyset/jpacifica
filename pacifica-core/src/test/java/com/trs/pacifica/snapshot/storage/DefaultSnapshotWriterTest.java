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

import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

public class DefaultSnapshotWriterTest extends BaseStorageTest {

    private DefaultSnapshotStorage snapshotStorage;

    private DefaultSnapshotWriter snapshotWriter;

    private LogId snapshotLogId = new LogId(1003, 1);

    private String tempSnapshotMame = "test_tmp";

    private String writerPath;

    private String snapshotPath;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        this.writerPath = this.path + File.separator + tempSnapshotMame;
        mockTest();
        this.snapshotStorage = new DefaultSnapshotStorage(this.path);
        this.snapshotPath = this.snapshotStorage.getSnapshotPath(snapshotLogId.getIndex());
        this.snapshotStorage = Mockito.spy(this.snapshotStorage);
        this.snapshotWriter = new DefaultSnapshotWriter(snapshotLogId, this.snapshotStorage, tempSnapshotMame);
    }


    @AfterEach
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }

    private void mockTest() {
        File writerDir = new File(this.writerPath);
        writerDir.mkdir();
    }

    private String getWriteFile(String filename) {
        return this.writerPath + File.separator + filename;
    }

    private File mockFile(String filename) throws IOException {
        String writeFileFilePath = getWriteFile(filename);
        File file = new File(writeFileFilePath);
        file.createNewFile();
        return file;
    }

    @Test
    public void testClose() throws IOException {
        Assertions.assertEquals(this.snapshotLogId, this.snapshotWriter.getSnapshotLogId());
        String path = this.snapshotWriter.getDirectory();
        Assertions.assertEquals(writerPath, path);
        String filename1 = "test1";
        File file1 = mockFile(filename1);
        this.snapshotWriter.addFile(filename1);

        String filename2 = "test2";
        File file2 = mockFile(filename2);
        this.snapshotWriter.addFile(filename2);
        this.snapshotWriter.close();
        Assertions.assertTrue(!file1.exists());
        Assertions.assertTrue(!file2.exists());
        Assertions.assertEquals(this.snapshotLogId.getIndex(), this.snapshotStorage.getLastSnapshotIndex());

        File snapshotDir = new File(this.snapshotPath);
        File snapshotFile1 = new File(snapshotDir, filename1);
        File snapshotFile2 = new File(snapshotDir, filename1);
        Assertions.assertTrue(snapshotFile1.exists());
        Assertions.assertTrue(snapshotFile2.exists());
        Assertions.assertThrows(AlreadyClosedException.class, () -> {
            this.snapshotWriter.ensureClosed();
        });


    }



}
