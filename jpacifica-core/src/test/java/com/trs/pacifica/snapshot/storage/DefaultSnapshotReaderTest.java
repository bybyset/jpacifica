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
import com.trs.pacifica.fs.FileService;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class DefaultSnapshotReaderTest extends BaseStorageTest {


    private DefaultSnapshotReader snapshotReader;

    private DefaultSnapshotStorage snapshotStorage;

    private LogId snapshotLogId = new LogId(1003, 1);

    private FileService fileService = Mockito.mock(FileService.class);

    private String snapshotName = DefaultSnapshotStorage.SNAPSHOT_DIRECTORY_PREFIX + "1003";


    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        this.snapshotStorage = new DefaultSnapshotStorage(this.path);
        this.snapshotStorage = Mockito.spy(snapshotStorage);
        mockTestSnapshot();
        this.snapshotReader = new DefaultSnapshotReader(this.snapshotStorage, snapshotName);

    }

    @AfterEach
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }


    private void mockTestSnapshot() throws IOException {
        Mockito.doReturn(1L).when(this.fileService).addFileReader(Mockito.any());
        Mockito.doReturn(true).when(this.fileService).removeFileReader(Mockito.anyLong());
        String snapshotPath = this.snapshotStorage.getSnapshotPath(snapshotName);
        File snapshotDir = new File(snapshotPath);
        if (!snapshotDir.exists()) {
            snapshotDir.mkdir();
        }
        DefaultSnapshotMeta snapshotMeta = new DefaultSnapshotMeta(snapshotLogId);
        snapshotMeta.addFile("test1");
        snapshotMeta.addFile("test2");
        String snapshotMetaFile = DefaultSnapshotMeta.getSnapshotMetaFilePath(snapshotPath);
        DefaultSnapshotMeta.saveToFile(snapshotMeta, snapshotMetaFile, true);

    }

    @Test
    public void testGetSnapshotLogId() {
        Assertions.assertEquals(snapshotLogId, this.snapshotReader.getSnapshotLogId());
    }

    @Test
    public void testGetSnapshotPath() {
        String expectPath = this.path + File.separator + snapshotName;
        Assertions.assertEquals(expectPath, this.snapshotReader.getDirectory());
    }

    @Test
    public void testListFile() {
        Collection<String> files = this.snapshotReader.listFiles();
        Assertions.assertTrue(files.contains("test1"));
        Assertions.assertTrue(files.contains("test2"));
    }



    @Test
    public void testGenerateReadIdForDownload() {
        long readId = this.snapshotReader.generateReadIdForDownload(this.fileService);
        Assertions.assertTrue(readId > 0);
        Assertions.assertEquals(readId, this.snapshotReader.getFileReaderId());
        Mockito.verify(this.fileService).addFileReader(Mockito.any());
        long readIdSecond = this.snapshotReader.generateReadIdForDownload(this.fileService);
        Assertions.assertEquals(readId, readIdSecond);
    }

    @Test
    public void testClose() throws IOException {
        long readId = this.snapshotReader.generateReadIdForDownload(this.fileService);
        this.snapshotReader.close();
        Mockito.verify(this.fileService).removeFileReader(readId);
        Mockito.verify(this.snapshotStorage).destroySnapshot(Mockito.anyString());
        Assertions.assertThrows(AlreadyClosedException.class, () -> {
            this.snapshotReader.ensureClosed();
        });
    }


}
