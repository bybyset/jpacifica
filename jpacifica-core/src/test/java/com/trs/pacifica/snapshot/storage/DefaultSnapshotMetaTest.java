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
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DefaultSnapshotMetaTest extends BaseStorageTest {

    private DefaultSnapshotMeta defaultSnapshotMeta;

    private LogId snapshotLogId = new LogId(1003, 1);


    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        this.defaultSnapshotMeta = new DefaultSnapshotMeta(snapshotLogId);
        this.defaultSnapshotMeta = Mockito.spy(defaultSnapshotMeta);
    }

    @AfterEach
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }


    @Test
    public void testAddFile() {
        List<String> files = new ArrayList<>();
        files.add("test1");
        files.add("test2");
        files.add("test3");
        for (String filename : files) {
            this.defaultSnapshotMeta.addFile(filename);
        }
        Collection<String> filenames =this.defaultSnapshotMeta.listFiles();
        Assertions.assertNotNull(filenames);
        Assertions.assertEquals(files.size(), filenames.size());
        Assertions.assertTrue(filenames.stream().allMatch( filename -> {
            return files.contains(filename);
        }));

        this.defaultSnapshotMeta.removeFile("test2");

        Collection<String> filenames1 =this.defaultSnapshotMeta.listFiles();
        Assertions.assertNotNull(filenames);
        Assertions.assertEquals(2, filenames.size());


    }


    @Test
    public void testSaveAndLoadToFile() throws IOException {
        String filename1 = "test1";
        RpcCommon.FileMeta fileMeta1 = RpcCommon.FileMeta.newBuilder()//
                .setChecksum(12233)//
                .build();
        Assertions.assertTrue(this.defaultSnapshotMeta.addFile(filename1, fileMeta1));
        String filename2 = "test2";
        RpcCommon.FileMeta fileMeta2 = RpcCommon.FileMeta.newBuilder()//
                .setChecksum(33441)//
                .build();
        Assertions.assertTrue(this.defaultSnapshotMeta.addFile(filename2, fileMeta2));

        File svaeFile = new File(this.path + File.separator + "test_meta");
        DefaultSnapshotMeta.saveToFile(this.defaultSnapshotMeta, svaeFile.getPath(), true);


        DefaultSnapshotMeta loadSnapshotMeta = DefaultSnapshotMeta.loadFromFile(svaeFile);
        Assertions.assertEquals(this.snapshotLogId, loadSnapshotMeta.getSnapshotLogId());
        Assertions.assertEquals(fileMeta1, loadSnapshotMeta.getFileMeta(filename1));
        Assertions.assertEquals(fileMeta2, loadSnapshotMeta.getFileMeta(filename2));

    }

}
