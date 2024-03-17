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
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.error.PacificaLogEntryException;
import com.trs.pacifica.log.codec.LogEntryDecoder;
import com.trs.pacifica.log.codec.LogEntryEncoder;
import com.trs.pacifica.log.file.IndexFile;
import com.trs.pacifica.log.store.IndexStore;
import com.trs.pacifica.log.store.SegmentStore;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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
    public LogId getLogIdAt(final long index) {

        return null;
    }

    @Override
    public LogId getFirstLogId() {
        this.readLock.lock();
        try {
            final long firstLogIndex = this.segmentStore.getFirstLogIndex();
            if (firstLogIndex > 0) {
                final IndexFile indexFile = (IndexFile) this.indexStore.lookupFile(firstLogIndex);
                if (indexFile != null) {
                    final IndexFile.IndexEntry indexEntry = indexFile.lookupIndexEntry(firstLogIndex);
                    if (indexEntry != null) {
                        return indexEntry.getLogId();
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public LogId getLastLogId() {


        return null;
    }

    @Override
    public boolean appendLogEntry(LogEntry logEntry) throws PacificaException {
        return appendLogEntries(List.of(logEntry)) == 1;
    }

    @Override
    public int appendLogEntries(List<LogEntry> logEntries) throws PacificaException {
        if (logEntries == null || logEntries.isEmpty()) {
            return 0;
        }
        this.readLock.lock();
        try {
            final int totalCount = logEntries.size();
            int appendCount = 0;
            for (LogEntry logEntry : logEntries) {
                final byte[] logEntryBytes = this.logEntryEncoder.encode(logEntry);
                final LogId logId = logEntry.getLogId().copy();
                final boolean isWaitingFlush = appendCount == totalCount - 1;
                if (doAppendLogEntry(logId, logEntryBytes, isWaitingFlush)) {
                    appendCount++;
                } else {
                    // flush
                    break;
                }
            }
            return appendCount;
        } catch (IOException e) {
            throw new PacificaLogEntryException(String.format("store_path=%s append log entries(first_log_index=%d, count=%d) encountered an ", this.storagePath, logEntries.get(0).getLogId().getIndex(), logEntries.size()), e);
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean doAppendLogEntry(final LogId logId, final byte[] data, final boolean isWaitingFlush) throws IOException {
        Objects.requireNonNull(logId, "logId");
        if (this.segmentStore == null || this.indexStore == null) {
            return false;
        }
        final long logIndex = logId.getIndex();
        //write segment
        final Tuple2<Integer, Long> segmentResult = this.segmentStore.appendLogData(logIndex, data);
        if (segmentResult.getFirst() < 0 || segmentResult.getSecond() < 0) {
            return false;
        }
        //write index
        final Tuple2<Integer, Long> indexResult = this.indexStore.appendLogIndex(logId, segmentResult.getFirst());
        if (indexResult.getFirst() < 0 || indexResult.getSecond() < 0) {
            return false;
        }
        if (isWaitingFlush) {
            //TODO
        }
        return false;
    }

    private boolean waitForFlush(final long exceptedLogPosition, final long exceptedIndexPosition) {

        return false;
    }

    @Override
    public LogId truncatePrefix(long firstIndexKept) {
        this.writeLock.lock();
        try {
            this.segmentStore.truncatePrefix(firstIndexKept);
            this.indexStore.truncatePrefix(firstIndexKept);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.writeLock.unlock();
        }

        return new LogId(0, 0);
    }

    @Override
    public LogId truncateSuffix(long lastIndexKept) {
        this.writeLock.lock();
        try {
            this.segmentStore.truncateSuffix(lastIndexKept);
            this.indexStore.truncateSuffix(lastIndexKept);
        } finally {
            this.writeLock.unlock();
        }
        return new LogId(0, 0);
    }

    @Override
    public void close() {

    }
}
