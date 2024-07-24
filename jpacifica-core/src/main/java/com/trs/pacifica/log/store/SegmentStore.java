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

import com.trs.pacifica.log.codec.LogEntryDecoder;
import com.trs.pacifica.log.store.file.AbstractFile;
import com.trs.pacifica.log.store.file.Block;
import com.trs.pacifica.log.store.file.FileHeader;
import com.trs.pacifica.log.store.file.SegmentFile;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.util.Tuple2;
import com.trs.pacifica.util.io.DataBuffer;
import com.trs.pacifica.util.io.DataInput;
import com.trs.pacifica.util.io.LinkedDataBuffer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SegmentStore extends AbstractStore {

    static final String _FILE_SUFFIX = ".s";
    public static final String _DEFAULT_SEGMENT_DIR_NAME = "_log_segment";
    public static final int _DEFAULT_SEGMENT_FILE_SIZE = 32 * 1024 * 1024;// 32 M

    static final int DEFAULT_MIN_WRITE_BYTES = SegmentFile.getWriteByteSize(8);

    public SegmentStore(Path dir, int fileSize) throws IOException {
        super(dir, fileSize);
    }

    public SegmentStore(Path dir) throws IOException {
        this(dir, _DEFAULT_SEGMENT_FILE_SIZE);
    }

    /**
     * append byte array of the log data to segment file
     *
     * @param logIndex
     * @param logEntryData
     * @return two-tuples: (start write position of segment file, expect flush position)
     * @throws IOException
     */
    public Tuple2<Integer, Long> appendLogData(final long logIndex, final DataBuffer logEntryData) throws IOException {
        assert logEntryData != null;
        assert logIndex > 0;
        final long lastLogIndex = getLastLogIndex();
        if (lastLogIndex > 0 && logIndex != lastLogIndex + 1) {
            throw new IllegalArgumentException("expect logIndex=" + (lastLogIndex + 1) + ", but logIndex=" + logIndex);
        }
        final int minFreeByteSize = getMinWriteByteSize();
        int firstStartWritePos = -1;
        long expectFlushPos = -1L;
        int logEntryDataWritePos = 0;
        this.writeLock.lock();
        try {
            do {
                final AbstractFile lastFile = getLastFile(minFreeByteSize, true);
                if (lastFile != null && lastFile instanceof SegmentFile) {
                    final int startWritePos = lastFile.getWrotePosition();
                    final int writableBytes = lastFile.getFreeByteSize() - Block.HEADER_SIZE;
                    final int remaining = logEntryData.remaining();
                    final int maxWriteBytes = Math.min(remaining, writableBytes);
                    final DataBuffer writeData = logEntryData.slice(logEntryDataWritePos, maxWriteBytes);
                    final Block segmentBlock = SegmentFile.wrapBlock(writeData);
                    boolean hasNextBlock = writableBytes >= remaining ? false : true;
                    if (hasNextBlock) {
                        segmentBlock.setHasNextBlock();
                    }
                    final int realWriteBytes;
                    if (firstStartWritePos < 0) {
                        segmentBlock.setFirstBlock();
                        firstStartWritePos = startWritePos;
                        realWriteBytes = ((SegmentFile) lastFile).appendLogEntry(logIndex, segmentBlock);
                    } else {
                        realWriteBytes = ((SegmentFile) lastFile).appendLogEntry(segmentBlock);
                    }
                    logEntryDataWritePos += maxWriteBytes;
                    logEntryData.position(logEntryDataWritePos);
                    expectFlushPos = lastFile.getStartOffset() + startWritePos + realWriteBytes;
                }
            } while (logEntryData.hasRemaining());
        } finally {
            this.writeLock.unlock();
        }
        return Tuple2.of(firstStartWritePos, expectFlushPos);
    }


    /**
     * @param logIndex    index of log entry
     * @param logPosition position in segment file of log entry
     * @return null if not found
     */
    public DataBuffer lookupLogEntry(final long logIndex, int logPosition) throws IOException {
        //lookup segment file
        List<DataBuffer> dataBufferList = new ArrayList<>(2);
        SegmentFile segmentFile = (SegmentFile) this.lookupFile(logIndex);
        do {
            if (segmentFile == null) {
                break;
            }
            final Block block = segmentFile.lookupBlock(logPosition);
            if (block == null) {
                break;
            }
            dataBufferList.add(block.getLogEntryData());
            if (block.hasNextBlock()) {
                segmentFile = (SegmentFile) this.getNextFile(segmentFile);
                logPosition = FileHeader.getBytesSize();
            } else {
                break;
            }
        } while (segmentFile != null);
        return new LinkedDataBuffer(dataBufferList);
    }

    @Override
    protected String getFileSuffix() {
        return _FILE_SUFFIX;
    }

    @Override
    protected AbstractFile newAbstractFile(String filename) throws IOException {
        return new SegmentFile(directory, filename);
    }

    /**
     * @param startLogIndex
     * @param startLogPosition
     * @param endLogIndex
     * @return
     */
    public LogEntryIterator sliceLogEntry(final LogEntryDecoder logEntryDecoder, final long startLogIndex, final int startLogPosition, final long endLogIndex) {
        SegmentFile segmentFile = (SegmentFile) this.lookupFile(startLogIndex);
        if (segmentFile != null) {
            List<AbstractFile> slice = this.sliceFile(segmentFile);
            AbstractFile[] files = new AbstractFile[slice.size()];
            slice.toArray(files);
            return new LogEntryIteratorImpl(logEntryDecoder, files, startLogIndex, startLogPosition, endLogIndex);
        }
        return null;
    }

    public LogEntryIterator sliceLogEntry(final LogEntryDecoder logEntryDecoder, final long startLogIndex, final int startLogPosition) {
        final long lastLogIndex = getLastLogIndex();
        return sliceLogEntry(logEntryDecoder, startLogIndex, startLogPosition, lastLogIndex);
    }

    static int getAppendLogDataByteSize(final DataInput logDataInput) {
        return SegmentFile.getWriteByteSize(logDataInput.getByteSize());
    }

    /**
     * When appending the log, at least the number of bytes to write,
     * when the end of the file is not enough to write, we will skip writing to the file and fill the end.
     *
     * @return
     */
    static int getMinWriteByteSize() {
        return DEFAULT_MIN_WRITE_BYTES;
    }

    public static interface LogEntryIterator extends Iterator<LogEntry> {

        /**
         * get start position of the current log entry at segment file
         * @return -1 if log entry is null
         */
        int getLogEntryStartPosition();

    }

    class LogEntryIteratorImpl implements LogEntryIterator {

        private final LogEntryDecoder logEntryDecoder;
        private final AbstractFile[] files;
        private final long endLogIndex;
        private long currentLogIndex;
        private int currentFileIndex = 0;
        private int currentPosition;

        private LogEntry currentLogEntry = null;

        private int currentLogEntryPosition = -1;

        public LogEntryIteratorImpl(LogEntryDecoder logEntryDecoder, AbstractFile[] files, long startLogIndex, int startLogPosition, long endLogIndex) {
            this.logEntryDecoder = logEntryDecoder;
            this.files = files;
            this.endLogIndex = endLogIndex;
            this.currentLogIndex = startLogIndex;
            this.currentPosition = startLogPosition;
        }

        @Override
        public boolean hasNext() {
            return this.currentFileIndex < this.files.length && currentLogIndex < this.endLogIndex;
        }

        @Override
        public LogEntry next() {
            SegmentStore.this.ensureOpen();
            List<DataBuffer> dataBufferList = new ArrayList<>(2);

            do {
                if (this.currentFileIndex >= this.files.length) {
                    this.currentLogEntry = null;
                    this.currentLogEntryPosition = -1;
                    return null;
                }
                final SegmentFile currentFile = (SegmentFile) this.files[currentFileIndex];
                try {
                    final Block block = currentFile.lookupBlock(this.currentPosition);
                    if (block == null) {
                        // next file
                        nextFile();
                        continue;
                    }
                    if (block.isFirstBlock()) {
                        this.currentLogEntryPosition = this.currentPosition;
                    }
                    dataBufferList.add(block.getLogEntryData());
                    this.currentPosition += block.getByteSize();
                    if (!block.hasNextBlock()) {
                        break;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            } while (true);
            this.currentLogIndex++;
            this.currentLogEntry = this.logEntryDecoder.decode(new LinkedDataBuffer(dataBufferList));
            return this.currentLogEntry;
        }

        private void nextFile() {
            this.currentFileIndex++;
            this.currentPosition = FileHeader.getBytesSize();
        }

        @Override
        public int getLogEntryStartPosition() {
            return currentLogEntryPosition;
        }
    }

}
