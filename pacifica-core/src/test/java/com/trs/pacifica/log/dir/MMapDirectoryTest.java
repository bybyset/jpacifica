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

package com.trs.pacifica.log.dir;

import com.trs.pacifica.log.io.InOutput;
import com.trs.pacifica.test.BaseStorageTest;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

class MMapDirectoryTest extends BaseStorageTest {

    MMapDirectory mMapDirectory;

    @BeforeEach
    public void setUp() throws Exception {
        File dir = new File(this.path);
        this.mMapDirectory = (MMapDirectory) MMapDirectory.open(dir.toPath());

    }

    @Test
    void testCreateFile() throws IOException {
        String filename = "test_create_file";
        int fileSize = 10240;
        this.mMapDirectory.createFile(filename, fileSize);
        File file = new File(this.path, filename);
        Assertions.assertTrue(file.exists());
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(fileSize, file.length());
    }

    @Test
    void testListAll() throws IOException {
        String filename1 = "test_list_all_file_1";
        String filename2 = "test_list_all_file_2";
        int fileSize = 10240;
        this.mMapDirectory.createFile(filename1, fileSize);
        this.mMapDirectory.createFile(filename2, fileSize);

        String[] filenames = this.mMapDirectory.listAll();

        Assertions.assertTrue(Arrays.stream(filenames).anyMatch( (filename) -> {
            return filename.equals(filename1);
        }));

        Assertions.assertTrue(Arrays.stream(filenames).anyMatch( (filename) -> {
            return filename.equals(filename2);
        }));
    }

    @Test
    void testDeleteFile() throws IOException {
        String filename1 = "test_delete_file_1";
        String filename2 = "test_delete_file_2";
        int fileSize = 10240;
        this.mMapDirectory.createFile(filename1, fileSize);
        this.mMapDirectory.createFile(filename2, fileSize);

        try (final InOutput inOutput = this.mMapDirectory.openInOutput(filename1)){
            Assertions.assertNotNull(inOutput);
            Assertions.assertNotNull(this.mMapDirectory.getFileInOutput(filename1));
        }
        File file1 = new File(this.path, filename1);
        Assertions.assertTrue(file1.exists());
        this.mMapDirectory.deleteFile(filename1);
        Assertions.assertFalse(file1.exists());
        Assertions.assertNull(this.mMapDirectory.getFileInOutput(filename1));

    }

    @Test
    void testOpenInOutput() throws IOException {
        String filename1 = "test_file_1";
        int fileSize = 4 << 10;// 4m
        this.mMapDirectory.createFile(filename1, fileSize);
        final byte[] writeBytes = "test_write_bytes".getBytes();
        try (final InOutput inOutput = this.mMapDirectory.openInOutput(filename1)){
            Assertions.assertNotNull(inOutput);
            Assertions.assertNotNull(this.mMapDirectory.getFileInOutput(filename1));
            inOutput.writeBytes(0, writeBytes);
        }
        this.mMapDirectory.sync(filename1);

        final byte[] readBytes = new byte[writeBytes.length];
        try (final InOutput inOutput = this.mMapDirectory.openInOutput(filename1)){
            Assertions.assertNotNull(inOutput);
            Assertions.assertNotNull(this.mMapDirectory.getFileInOutput(filename1));
            inOutput.seek(0);
            inOutput.readBytes(readBytes);
        }
        Assertions.assertArrayEquals(writeBytes, readBytes);

    }

}
