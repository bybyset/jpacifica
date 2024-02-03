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
import com.trs.pacifica.log.file.IndexFile;
import com.trs.pacifica.util.Tuple2;

import java.io.IOException;

import static com.trs.pacifica.log.store.FileType.INDEX;

public class IndexStore extends AbstractStore{


    public IndexStore(FileType type) {
        super(INDEX);
    }

    /**
     * build an index for LogEntry, so that it is easy to read the log bytes through logIndex.
     *
     * @param logIndex  log index of LogEntry
     * @param logPosition start position in segment file about LogEntry
     * @return two-tuples: (start write position of segment file, expect flush position)
     * @exception IllegalArgumentException we have to submit in the order of log index, otherwise throw
     */
    public Tuple2<Integer, Long> appendLogIndex(final long logIndex, final int logPosition) throws IOException {
        //TODO How is the order of logIndex guaranteed for concurrency, or do we not need to consider the order?
        final long lastLogIndex = getLastLogIndex();
        if (logIndex != lastLogIndex + 1) {
            throw  new IllegalArgumentException("expect logIndex=" + lastLogIndex + 1 + ", but logIndex=" + logIndex);
        }
        final int minFreeByteSize = IndexFile.getWriteByteSize();
        final AbstractFile lastFile = getLastFile(minFreeByteSize , true);
        if (lastFile != null && lastFile instanceof IndexFile) {
            final int startWritePos = ((IndexFile)lastFile).appendIndexData(logIndex, logPosition);
            final long expectFlushPos = lastFile.getStartOffset() + startWritePos + minFreeByteSize;
            return Tuple2.of(startWritePos, expectFlushPos);
        }
        return Tuple2.of(-1, -1L);
    }


    @Override
    protected AbstractFile doAllocateFile(String filename) throws IOException {
        return null;
    }
}
