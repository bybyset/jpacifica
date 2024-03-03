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
import com.trs.pacifica.log.file.FileHeader;
import com.trs.pacifica.log.file.IndexFile;
import com.trs.pacifica.log.file.SegmentFile;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.Tuple2;

import java.io.IOException;
import java.nio.file.Path;

public class IndexStore extends AbstractStore {

    static final String _FILE_SUFFIX = ".i";

    public static final String _DEFAULT_INDEX_DIR_NAME = "log_index";
    public static final int _DEFAULT_INDEX_ENTRY_COUNT_PER_FILE = 1000;

    static final int _DEFAULT_SEGMENT_FILE_SIZE = FileHeader.getBytesSize() + _DEFAULT_INDEX_ENTRY_COUNT_PER_FILE * IndexFile.getWriteByteSize();

    private final int fileSize;

    public IndexStore(Path dir, int indexEntryCountPerFile) throws IOException {
        super(dir);
        this.fileSize = FileHeader.getBytesSize() + indexEntryCountPerFile * IndexFile.getWriteByteSize();;
    }

    public IndexStore(Path dir) throws IOException {
        this(dir, _DEFAULT_INDEX_ENTRY_COUNT_PER_FILE);
    }

    @Override
    protected String getFileSuffix() {
        return _FILE_SUFFIX;
    }

    /**
     * build an index for LogEntry, so that it is easy to read the log bytes through logIndex.
     *
     * @param logId    LogId of LogEntry
     * @param logPosition start position in segment file about LogEntry
     * @return two-tuples: (start write position of segment file, expect flush position)
     * @throws IllegalArgumentException we have to submit in the order of log index, otherwise throw
     */
    public Tuple2<Integer, Long> appendLogIndex(final LogId logId, final int logPosition) throws IOException {
        //TODO How is the order of logIndex guaranteed for concurrency, or do we not need to consider the order?
        final long logIndex = logId.getIndex();
        final long lastLogIndex = getLastLogIndex();
        if (logIndex != lastLogIndex + 1) {
            throw new IllegalArgumentException("expect logIndex=" + lastLogIndex + 1 + ", but logIndex=" + logIndex);
        }
        final int minFreeByteSize = IndexFile.getWriteByteSize();
        final AbstractFile lastFile = getLastFile(minFreeByteSize, true);
        if (lastFile != null && lastFile instanceof IndexFile) {
            final int startWritePos = ((IndexFile) lastFile).appendIndexData(logId, logPosition);
            final long expectFlushPos = lastFile.getStartOffset() + startWritePos + minFreeByteSize;
            return Tuple2.of(startWritePos, expectFlushPos);
        }
        return Tuple2.of(-1, -1L);
    }


    @Override
    protected AbstractFile doAllocateFile(String filename) throws IOException {
        this.directory.createFile(filename, this.fileSize);
        return new IndexFile(this.directory, filename);
    }


    /**
     * lookup position of the log in segment file
     * @param index  sequence number of log
     * @return _NOT_FOUND
     */
    public int lookupPositionAt(long index) {
        // lookup IndexFile

        //

        return AbstractFile._NOT_FOUND;
    }
}
