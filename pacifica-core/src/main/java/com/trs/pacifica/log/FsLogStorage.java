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

package com.trs.pacifica.log;

import com.trs.pacifica.LogStorage;
import com.trs.pacifica.log.codec.LogEntryDecoder;
import com.trs.pacifica.log.codec.LogEntryEncoder;
import com.trs.pacifica.log.store.IndexStore;
import com.trs.pacifica.log.store.SegmentStore;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * base on FileSystem implement
 */
public class FsLogStorage implements LogStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(FsLogStorage.class);

    private final String storagePath;

    private final LogEntryDecoder logEntryDecoder;

    private final LogEntryEncoder logEntryEncoder;

    private final IndexStore indexStore;

    private final SegmentStore segmentStore;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private final FsLogStorageOption option;


    public FsLogStorage(String storagePath, LogEntryDecoder logEntryDecoder, LogEntryEncoder logEntryEncoder, FsLogStorageOption option) throws IOException {
        this.storagePath = Objects.requireNonNull(storagePath, "storagePath");
        this.logEntryDecoder = logEntryDecoder;
        this.logEntryEncoder = logEntryEncoder;
        this.option = option;
        File storageDir = new File(storagePath);
        if (!storageDir.exists() && !storageDir.mkdir()) {
            throw new IOException(String.format("storagePath=%s not is directory.", storagePath));
        }
        final Path path = storageDir.toPath();
        this.indexStore = new IndexStore(path.resolve(this.option.getIndexDirName()), this.option.getIndexEntryCountPerFile());
        this.segmentStore = new SegmentStore(path.resolve(this.option.getSegmentDirName()), this.option.getSegmentFileSize());
    }

    public FsLogStorage(String storagePath, LogEntryDecoder logEntryDecoder, LogEntryEncoder logEntryEncoder) throws IOException {
        this(storagePath, logEntryDecoder, logEntryEncoder, new FsLogStorageOption());
    }


    @Override
    public LogEntry getLogEntry(long index) {
        //validate

        //look index  at IndexStore

        final int logPosition = this.indexStore.lookupPositionAt(index);

        //look LogEntry bytes at SegmentStore

        final byte[] logEntryBytes = this.segmentStore.lookupLogEntry(index, logPosition);

        // decode LogEntry bytes
        this.logEntryDecoder.decode(logEntryBytes);

        return null;
    }

    @Override
    public LogId getLogIdAt(int index) {
        return null;
    }

    @Override
    public LogId getFirstLogIndex() {
        return null;
    }

    @Override
    public LogId getLastLogIndex() {
        return null;
    }
    @Override
    public boolean appendLogEntry(LogEntry logEntry) {
        return appendLogEntries(List.of(logEntry)) == 1;
    }

    @Override
    public int appendLogEntries(List<LogEntry> logEntries) {
        if (logEntries == null || logEntries.isEmpty()) {
            return 0;
        }
        this.readLock.lock();
        try {
            final int totalCount = logEntries.size();
            int appendCount = 0;
            for (LogEntry logEntry : logEntries) {
                final byte[] logEntryBytes = this.logEntryEncoder.encode(logEntry);
                final long logIndex = logEntry.getLogId().getIndex();
                final boolean isWaitingFlush = appendCount == totalCount - 1;
                if (doAppendLogEntry(logIndex, logEntryBytes, isWaitingFlush)) {
                    appendCount++;
                } else {
                    // flush
                    break;
                }
            }
            return appendCount;
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean doAppendLogEntry(final long logIndex, final byte[] data, final boolean isWaitingFlush) {
        if (this.segmentStore == null || this.indexStore == null) {
            return false;
        }
        try {
            //write segment
            final Tuple2<Integer, Long> segmentResult = this.segmentStore.appendLogData(logIndex, data);
            if (segmentResult.getFirst() < 0 || segmentResult.getSecond() < 0) {
                return false;
            }
            //write index
            final Tuple2<Integer, Long> indexResult = this.indexStore.appendLogIndex(logIndex, segmentResult.getFirst());
            if (indexResult.getFirst() < 0 || indexResult.getSecond() < 0) {
                return false;
            }
            if (isWaitingFlush) {
                //TODO
            }
        } catch (Throwable e){
            LOGGER.error("path={} failed to append LogEntry(index={})", this.storagePath, logIndex);
        }
        return false;
    }

    private boolean waitForFlush(final long exceptedLogPosition, final long exceptedIndexPosition) {

        return false;
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        return false;
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        return false;
    }

    @Override
    public void close() {

    }
}
