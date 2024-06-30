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

import com.trs.pacifica.fs.remote.RemoteFileDownloader;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DefaultSnapshotDownloaderTest extends BaseStorageTest {


    private RemoteFileDownloader remoteFileDownloader;

    private DefaultSnapshotStorage snapshotStorage;

    private DefaultSnapshotDownloader snapshotDownloader;

    private DefaultSnapshotWriter snapshotWriter;
    private long remoteReaderId = 1L;

    private final LogId remoteLogId = new LogId(1003, 1);

    private Executor executor = Executors.newFixedThreadPool(4);

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        this.remoteFileDownloader = Mockito.mock(RemoteFileDownloader.class);
        Answer answer = (invocation) -> {
            String filename = invocation.getArgument(0, String.class);
            if (filename.equals(DefaultSnapshotMeta.SNAPSHOT_META_FILE)) {
                File saveFile = invocation.getArgument(1, File.class);
                DefaultSnapshotMeta defaultSnapshotMeta = new DefaultSnapshotMeta(remoteLogId);
                defaultSnapshotMeta.addFile("test1");
                defaultSnapshotMeta.addFile("test2");
                DefaultSnapshotMeta.saveToFile(defaultSnapshotMeta, saveFile.getPath(), true);
            } else {
                File saveFile = invocation.getArgument(1, File.class);
                saveFile.createNewFile();
            }

            return null;
        };
        Mockito.doAnswer(answer).when(this.remoteFileDownloader).asyncDownloadToFile(Mockito.anyString(), Mockito.any(File.class), Mockito.anyInt(), Mockito.any());
        Mockito.doAnswer(answer).when(this.remoteFileDownloader).downloadToFile(Mockito.anyString(), Mockito.any());
        this.snapshotWriter = Mockito.mock(DefaultSnapshotWriter.class);
        Mockito.doReturn(this.path).when(this.snapshotWriter).getDirectory();
        this.snapshotDownloader = new DefaultSnapshotDownloader(this.snapshotWriter, this.remoteFileDownloader, 10000, executor);
    }


    @AfterEach
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }

    @Test
    public void testStart() throws ExecutionException, InterruptedException {
        this.snapshotDownloader.start();
        this.snapshotDownloader.awaitComplete();
        File file1 = new File(this.path, "test1");
        File file2 = new File(this.path, "test2");
        Assertions.assertTrue(file1.exists());
        Assertions.assertTrue(file2.exists());
    }


}
