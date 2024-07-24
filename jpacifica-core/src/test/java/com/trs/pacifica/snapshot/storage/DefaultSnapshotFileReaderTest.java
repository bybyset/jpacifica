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

import com.trs.pacifica.fs.FileReader;
import com.trs.pacifica.test.BaseStorageTest;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DefaultSnapshotFileReaderTest extends BaseStorageTest {


    DefaultSnapshotFileReader defaultSnapshotFileReader;

    private String snapshotPath;

    private String filename;

    private byte[] fileContent;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        mockForTest();
        this.defaultSnapshotFileReader = new DefaultSnapshotFileReader(this.snapshotPath);
    }

    @AfterEach
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }


    private void mockForTest() throws IOException {
        this.filename = "test1";
        this.snapshotPath = this.path + File.separator + "snapshot_1133";
        File snapshotDir = new File(snapshotPath);
        snapshotDir.mkdir();
        File snapshotFile = new File(snapshotDir, this.filename);
        this.fileContent = "this is test content for DefaultSnapshotFileReader".getBytes("utf-8");
        FileUtils.writeByteArrayToFile(snapshotFile, fileContent);

    }


    @Test
    void testRead() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(this.fileContent.length);
        int len = this.defaultSnapshotFileReader.read(buffer, this.filename, 0, this.fileContent.length);
        Assertions.assertEquals(this.fileContent.length, len);
        Assertions.assertArrayEquals(this.fileContent, buffer.array());
    }

    @Test
    void testReadMultiBatch() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(this.fileContent.length);
        int offset = 0;
        int batch = 5;
        int len = -1;
        while ((len = this.defaultSnapshotFileReader.read(buffer, this.filename, offset, batch)) != DefaultSnapshotFileReader.EOF) {
            offset += len;
        }
        Assertions.assertEquals(FileReader.EOF, len);
        Assertions.assertEquals(this.fileContent.length, buffer.limit());
        Assertions.assertArrayEquals(this.fileContent, buffer.array());
    }



}
