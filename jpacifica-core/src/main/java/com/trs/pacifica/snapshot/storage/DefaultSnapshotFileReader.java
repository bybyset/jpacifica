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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DefaultSnapshotFileReader implements FileReader {

    private final String snapshotPath;

    public DefaultSnapshotFileReader(String snapshotPath) {
        this.snapshotPath = snapshotPath;
    }


    @Override
    public int read(ByteBuffer buffer, String filename, int offset, int length) throws IOException {
        final String readFilePath = getReadFilePath(filename);
        File readFile = new File(readFilePath);
        if (!readFile.exists()) {
            return EOF;
        }
        try (final FileInputStream input = new FileInputStream(readFile);
             final FileChannel fc = input.getChannel()) {
            int readLen = fc.read(buffer, offset);
            if (readLen <= 0) {
                return EOF;
            }
            return readLen;
        }
    }

    private String getReadFilePath(String filename) {
        return this.snapshotPath + File.separator + filename;
    }
}
