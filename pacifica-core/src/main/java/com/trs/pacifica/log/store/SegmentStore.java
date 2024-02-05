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

import com.trs.pacifica.log.file.AbstractFile;
import com.trs.pacifica.log.file.SegmentFile;
import com.trs.pacifica.util.Tuple2;

import java.io.IOException;
import java.nio.file.Path;

public class SegmentStore extends AbstractStore {

    static final String _FILE_SUFFIX = ".s";
    static final int _DEFAULT_SEGMENT_FILE_SIZE = 32 * 1024 * 1024;

    private final int fileSize;

    public SegmentStore(Path dir, int fileSize) throws IOException {
        super(dir);
        this.fileSize = fileSize;
    }

    public SegmentStore(Path dir) throws IOException {
        this(dir, _DEFAULT_SEGMENT_FILE_SIZE);
    }

    /**
     * append byte array of the log data to segment file
     *
     * @param logIndex
     * @param logData
     * @return two-tuples: (start write position of segment file, expect flush position)
     * @throws IOException
     */
    public Tuple2<Integer, Long> appendLogData(final long logIndex, final byte[] logData) throws IOException {
        final int minFreeByteSize = SegmentFile.getWriteByteSize(logData);
        final AbstractFile lastFile = getLastFile(minFreeByteSize, true);
        if (lastFile != null && lastFile instanceof SegmentFile) {
            final int startWritePos = ((SegmentFile) lastFile).appendSegmentData(logIndex, logData);
            final long expectFlushPos = lastFile.getStartOffset() + startWritePos + minFreeByteSize;
            return Tuple2.of(startWritePos, expectFlushPos);
        }
        return Tuple2.of(-1, -1L);
    }


    @Override
    protected String getFileSuffix() {
        return _FILE_SUFFIX;
    }

    @Override
    protected AbstractFile doAllocateFile(String filename) throws IOException {
        this.directory.createFile(filename, this.fileSize);
        return new SegmentFile(this.directory, filename);
    }


}
