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
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.error.PacificaLogEntryException;
import com.trs.pacifica.log.codec.LogEntryDecoder;
import com.trs.pacifica.log.codec.LogEntryEncoder;
import com.trs.pacifica.log.store.file.FileHeader;
import com.trs.pacifica.log.store.file.IndexEntry;
import com.trs.pacifica.log.store.file.IndexFile;
import com.trs.pacifica.log.store.IndexStore;
import com.trs.pacifica.log.store.SegmentStore;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.IOUtils;
import com.trs.pacifica.util.ThrowsUtil;
import com.trs.pacifica.util.Tuple2;
import com.trs.pacifica.util.io.DataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
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


    public FsLogStorage(String storagePath, LogEntryEncoder logEntryEncoder, LogEntryDecoder logEntryDecoder, FsLogStorageOption option) throws IOException {
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

    public FsLogStorage(String storagePath, LogEntryEncoder logEntryEncoder, LogEntryDecoder logEntryDecoder) throws IOException {
        this(storagePath, logEntryEncoder, logEntryDecoder, new FsLogStorageOption());
    }


    @Override
    public void open() throws PacificaException {
        this.writeLock.lock();
        try {
            this.indexStore.load();
            this.segmentStore.load();
            // Check for consistency between IndexStore and SegmentStore
            if (!checkConsistencyAndAlign()) {
                throw new PacificaException(PacificaErrorCode.CONFLICT_LOG, "Check for consistency between IndexStore and SegmentStore, but failed to align.");
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("success to open storage_path={}.", this.storagePath);
            }
        } catch (IOException e) {
            throw new PacificaException(PacificaErrorCode.IO, String.format("failed to open FsLogStorage, error_msg=%s", e.getMessage()), e);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void close() throws PacificaException {
        this.writeLock.lock();
        try {
            Throwable th = null;
            try {
                IOUtils.close(this.segmentStore);
            } catch (IOException e) {
                th = e;
            }
            try {
                IOUtils.close(this.indexStore);
            } catch (IOException e) {
                th = ThrowsUtil.addSuppressed(th, e);
            }
            if (th != null) {
                throw new PacificaException(PacificaErrorCode.IO, th.getMessage(), th);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Check for consistency between IndexStore and SegmentStore.
     * we will align them to be consistent
     */
    private boolean checkConsistencyAndAlign() throws IOException {
        final long lastIndex = this.indexStore.getLastLogIndex();
        final long lastSegmentIndex = this.segmentStore.getLastLogIndex();
        if (lastIndex == lastSegmentIndex) {
            return true;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}, not consistency, last_index({}) of IndexStore, last_index({}) of SegmentStore", storagePath, lastIndex, lastSegmentIndex);
        }
        if (lastIndex > lastSegmentIndex) {
            //case 1: lastIndex > lastSegmentIndex
            //align IndexStore to the index of lastSegmentIndex
            return this.indexStore.truncateSuffix(Math.max(0, lastSegmentIndex));
        } else {
            //case 2: lastSegmentIndex > lastIndex
            //we should generate IndexEntry array sorted by index from SegmentStore, then store indexEntry array to IndexStore.
            //1.look position in SegmentFile at lastIndex
            long startLogIndex = lastIndex;
            int startIndexPosition = FileHeader.getBytesSize();
            if (lastIndex > 0) {
                startIndexPosition = this.indexStore.lookupPositionAt(lastIndex);
            } else {
                startLogIndex = this.segmentStore.getFirstLogIndex();
                //first log position
                startIndexPosition = this.segmentStore.getFirstLogPosition();
            }
            //2.list IndexEntry array after the lastIndex
            SegmentStore.LogEntryIterator logEntryIterator = this.sliceLogEntry(startLogIndex, startIndexPosition);
            List<IndexEntry> indexEntries = toIndexEntryList(logEntryIterator);
            //3.append IndexEntry array to IndexStore
            if (lastIndex > 0) {
                startLogIndex++;
            }
            Tuple2<Integer, Long> lastAppendResult = null;
            for (IndexEntry indexEntry : indexEntries) {
                if (startLogIndex++ == indexEntry.getLogId().getIndex()) {
                    lastAppendResult = this.indexStore.appendLogIndex(indexEntry);
                }
            }
            assert lastAppendResult != null;
            //4.flush IndexStore
            return this.indexStore.waitForFlush(lastAppendResult.getSecond(), 5);
        }


    }

    private SegmentStore.LogEntryIterator sliceLogEntry(final long startLogIndex, final int position) {
        return this.segmentStore.sliceLogEntry(this.logEntryDecoder, startLogIndex, position);
    }

    private static List<IndexEntry> toIndexEntryList(SegmentStore.LogEntryIterator logEntryIterator) {
        final List<IndexEntry> indexEntries = new ArrayList<>();
        while (logEntryIterator.hasNext()) {
            final LogEntry logEntry = logEntryIterator.next();
            assert logEntry != null;
            final int startPosition = logEntryIterator.getLogEntryStartPosition();
            indexEntries.add(new IndexEntry(logEntry.getLogId().copy(), startPosition));
        }
        return indexEntries;
    }

    @Override
    public LogEntry getLogEntry(long index) {
        if (index > 0) {
            // TODO if out of range ??
            //look index  at IndexStore
            final int logPosition = this.indexStore.lookupPositionAt(index);
            if (logPosition == IndexFile._NOT_FOUND) {
                return null;
            }
            //look LogEntry bytes at SegmentStore
            try {
                final DataBuffer logEntryBytes = this.segmentStore.lookupLogEntry(index, logPosition);
                if (logEntryBytes != null) {
                    // decode LogEntry bytes
                    return this.logEntryDecoder.decode(logEntryBytes);
                }
            } catch (IOException e) {

            }
        }
        return null;
    }


    /**
     * look LogId at index from IndexStore
     *
     * @param index
     * @return null if not found
     * @throws IOException
     */
    private LogId lookLogIdFromIndexStore(final long index) throws IOException {
        assert index > 0;
        final IndexFile indexFile = (IndexFile) this.indexStore.lookupFile(index);
        if (indexFile != null) {
            final IndexEntry indexEntry = indexFile.lookupIndexEntry(index);
            if (indexEntry != null) {
                return indexEntry.getLogId();
            }
        }
        return null;
    }

    @Override
    public LogId getLogIdAt(final long index) {
        this.readLock.lock();
        try {
            return lookLogIdFromIndexStore(index);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogId getFirstLogId() {
        this.readLock.lock();
        try {
            final long firstLogIndex = this.segmentStore.getFirstLogIndex();
            if (firstLogIndex > 0) {
                return lookLogIdFromIndexStore(firstLogIndex);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogId getLastLogId() {
        this.readLock.lock();
        try {
            final long firstLogIndex = this.segmentStore.getLastLogIndex();
            if (firstLogIndex > 0) {
                return lookLogIdFromIndexStore(firstLogIndex);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.readLock.unlock();
        }
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
                final DataBuffer logEntryData = this.logEntryEncoder.encode(logEntry);
                final LogId logId = logEntry.getLogId().copy();
                final boolean isWaitingFlush = appendCount == totalCount - 1;
                if (doAppendLogEntry(logId, logEntryData, isWaitingFlush)) {
                    appendCount++;
                } else {
                    // flush
                    break;
                }
            }
            return appendCount;
        } catch (IOException e) {
            throw new RuntimeException(String.format("store_path=%s append log entries(first_log_index=%d, count=%d) encountered an ", this.storagePath, logEntries.get(0).getLogId().getIndex(), logEntries.size()), e);
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean doAppendLogEntry(final LogId logId, final DataBuffer logEntryData, final boolean isWaitingFlush) throws IOException {
        Objects.requireNonNull(logId, "logId");
        if (this.segmentStore == null || this.indexStore == null) {
            return false;
        }
        final long logIndex = logId.getIndex();
        //write segment
        final Tuple2<Integer, Long> segmentResult = this.segmentStore.appendLogData(logIndex, logEntryData);
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
            if (!waitForFlush(segmentResult.getSecond(), indexResult.getSecond())) {
                return false;
            }
        }
        return true;
    }

    private boolean waitForFlush(final long exceptedLogPosition, final long exceptedIndexPosition) throws IOException {
        if (!this.segmentStore.waitForFlush(exceptedLogPosition, 5)) {
            return false;
        }
        if (!this.indexStore.waitForFlush(exceptedLogPosition, 5)) {
            return false;
        }
        return true;
    }

    @Override
    public LogId truncatePrefix(long firstIndexKept) {
        this.writeLock.lock();
        try {
            firstIndexKept = this.segmentStore.truncatePrefix(firstIndexKept);
            //TODO  if nothing at segment store we will clear index store
            this.indexStore.truncatePrefix(firstIndexKept);
            return new LogId(0, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public LogId truncateSuffix(long lastIndexKept) {
        this.writeLock.lock();
        try {
            int position = this.indexStore.lookupPositionAt(lastIndexKept);
            if (position > 0) {
                this.segmentStore.truncateSuffix(lastIndexKept, position);
            } else {
                this.segmentStore.truncateSuffix(lastIndexKept);
            }
            this.indexStore.truncateSuffix(lastIndexKept);
        } finally {
            this.writeLock.unlock();
        }
        return new LogId(0, 0);
    }

    @Override
    public boolean reset(long nextLogIndex) {
        return false;
    }


}
