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

import com.google.common.collect.Lists;
import com.trs.pacifica.log.dir.BaseDirectory;
import com.trs.pacifica.log.dir.FsDirectory;
import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.log.store.file.AbstractFile;
import com.trs.pacifica.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractStore implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStore.class);

    /**
     * Used to name new file.
     */
    private final AtomicLong nextFileSequence = new AtomicLong(0);
    private final Deque<AbstractFile> files = new ArrayDeque<>();
    protected final ReadWriteLock lock = new ReentrantReadWriteLock();
    protected final Lock readLock = lock.readLock();
    protected final Lock writeLock = lock.writeLock();
    protected final BaseDirectory directory;
    protected final int fileSize;
    protected final AtomicLong flushedPosition = new AtomicLong(0);

    protected volatile boolean closed = true;


    protected AbstractStore(Path dir, int fileSize) throws IOException {
        this.directory = FsDirectory.open(dir);
        this.fileSize = fileSize;
    }

    public void ensureOpen() throws AlreadyClosedException {
        if (this.closed) {
            throw new AlreadyClosedException(String.format("{} not opened.", this.getClass().getSimpleName()));
        }
    }

    public void load() throws IOException {
        this.writeLock.lock();
        try {
            if (this.closed) {
                loadExistedFiles();
                if (!this.files.isEmpty()) {
                    // set flushed position
                    final AbstractFile lastFile = this.files.peekLast();
                    this.setFlushedPosition(lastFile.getStartOffset() + lastFile.getFlushedPosition());
                }
                this.closed = false;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("success to load. after={}", this);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }


    /**
     * load existed files
     *
     * @return
     */
    protected void loadExistedFiles() throws IOException {
        final String[] filenames = this.directory.listAll();
        if (filenames == null || filenames.length == 0) {
            return;
        }
        List<AbstractFile> loadFiles = new ArrayList<>(filenames.length);
        //load forward from the last file
        Arrays.sort(filenames, Comparator.comparing(this::getFileSequenceFromFilename));
        AbstractFile lastFile = null;
        long nextFileSequence = 0;
        for (int i = filenames.length - 1; i >= 0; i--) {
            final String filename = filenames[i];

            final AbstractFile file = loadFile(filename, lastFile);
            if (file == null) {
                continue;
            }
            lastFile = file;
            loadFiles.add(file);
            nextFileSequence = Math.max(nextFileSequence, getFileSequenceFromFilename(filename) + 1);
        }
        this.nextFileSequence.set(nextFileSequence);
        // blank file we will set startOffset
        loadFiles = Lists.reverse(loadFiles);
        AbstractFile prevFile = null;
        for (AbstractFile file : loadFiles) {
            if (file.isBlank()) {
                file.setStartOffset(calculateFileStartOffset(prevFile));
            }
            prevFile = file;
            this.files.add(file);
        }
    }


    protected AbstractFile loadFile(final String filename, final AbstractFile nextFile) throws IOException {
        if (filename != null && filename.endsWith(getFileSuffix())) {
            AbstractFile abstractFile = newAbstractFile(filename);
            do {
                if (nextFile == null || !nextFile.isAvailable()) {
                    // the last file  or not contains anyone log entry(first block)
                    abstractFile.recover();
                    break;
                }
                abstractFile.setLastLogIndex(nextFile.getFirstLogIndex() - 1);
            } while (false);
            return abstractFile;
        }
        return null;
    }

    public void close() throws IOException {
        this.writeLock.lock();
        try {
            this.closed = true;
            this.files.clear();
            this.setFlushedPosition(0);
            IOUtils.close(this.directory);
        } finally {
            this.writeLock.unlock();
        }
    }


    private final String getNextFilename() {
        return String.format("%019d", this.nextFileSequence.getAndIncrement()) + getFileSuffix();
    }

    /**
     * get sequence number from filename
     *
     * @param filename
     * @return
     */
    long getFileSequenceFromFilename(final String filename) {
        Objects.requireNonNull(filename, "filename");
        final String fileSuffix = this.getFileSuffix();
        if (filename.endsWith(fileSuffix)) {
            int idx = filename.indexOf(fileSuffix);
            return Long.parseLong(filename.substring(0, idx));
        }
        return 0;
    }

    long getFileSequenceFromFile(final AbstractFile abstractFile) {
        Objects.requireNonNull(abstractFile, "abstractFile");
        return getFileSequenceFromFilename(abstractFile.getFilename());
    }

    /**
     * get file suffix when create next file
     *
     * @return
     */
    protected abstract String getFileSuffix();

    protected AbstractFile doAllocateFile(final String filename) throws IOException {
        this.directory.createFile(filename, this.fileSize);
        final AbstractFile abstractFile = newAbstractFile(filename);
        abstractFile.setStartOffset(calculateFileStartOffset());
        return abstractFile;
    }


    protected abstract AbstractFile newAbstractFile(final String filename) throws IOException;


    /**
     * create next file
     *
     * @return AbstractFile
     * @throws IOException
     */
    protected AbstractFile allocateNextFile() throws IOException {
        final String nextFilename = getNextFilename();
        return doAllocateFile(nextFilename);
    }

    /**
     * get next file of the specified currentFile.
     *
     * @param currentFile
     * @return null if there is no next file
     */
    public AbstractFile getNextFile(final AbstractFile currentFile) {
        ensureOpen();
        this.readLock.lock();
        try {
            if (this.files.isEmpty()) {
                return null;
            }
            if (currentFile == null) {
                return this.files.peekFirst();
            }
            AbstractFile[] fileArray = new AbstractFile[this.files.size()];
            this.files.toArray(fileArray);
            int index = Arrays.binarySearch(fileArray, currentFile, new Comparator<AbstractFile>() {
                @Override
                public int compare(AbstractFile left, AbstractFile right) {
                    return (int) (getFileSequenceFromFile(right) - getFileSequenceFromFile(left));
                }
            });
            if (index >= 0 && ++index < fileArray.length) {
                return fileArray[index];
            }
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    /**
     * get the file of last write
     *
     * @param minFreeByteSize   minimum number of free bytes of a file
     * @param createIfNecessary create
     * @return
     * @throws IOException
     */
    public AbstractFile getLastFile(final int minFreeByteSize, final boolean createIfNecessary) throws IOException {
        ensureOpen();
        AbstractFile lastFile = null;
        this.readLock.lock();
        try {
            lastFile = files.peekLast();
            if (lastFile != null && lastFile.getFreeByteSize() > minFreeByteSize) {
                return lastFile;
            }
        } finally {
            this.readLock.unlock();
        }
        //
        if (createIfNecessary) {
            this.writeLock.lock();
            try {
                if (lastFile != null) {
                    // fill end byte to lastFile
                    lastFile.fillEmptyBytesInFileEnd();
                }
                // do allocate file
                lastFile = allocateNextFile();
            } finally {
                this.writeLock.unlock();
            }
        }
        return lastFile;
    }


    /**
     * lookup a file that contains the specified logIndex
     *
     * @param logIndex sequence number of log, must be greater than 0, otherwise return null
     * @return null if not found
     */
    public AbstractFile lookupFile(final long logIndex) {
        ensureOpen();
        if (logIndex <= 0) {
            return null;
        }
        this.readLock.lock();
        try {
            if (this.files.isEmpty()) {
                return null;
            }
            if (this.files.size() == 1) {
                return this.files.peekFirst();
            }
            AbstractFile[] fileArray = new AbstractFile[this.files.size()];
            this.files.toArray(fileArray);
            int lo = 0, hi = fileArray.length - 1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                AbstractFile file = fileArray[mid];
                while (!file.isAvailable() && mid > lo) {
                    mid--;
                    file = fileArray[mid];
                }
                if (file.getLastLogIndex() < logIndex) {
                    lo = mid + 1;
                } else if (file.getFirstLogIndex() > logIndex) {
                    hi = mid - 1;
                } else {
                    return file;
                }
            }
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    public List<AbstractFile> sliceFile(final AbstractFile startFile) {
        if (startFile != null) {
            List<AbstractFile> slice = new ArrayList<>(this.files.size());
            boolean find = false;
            for (AbstractFile file : this.files) {
                if (find || startFile == file) {
                    slice.add(file);
                    find = true;
                }
            }
            return slice;
        }
        return Collections.emptyList();
    }


    /**
     * get first log index
     *
     * @return -1L if nothing
     */
    public long getFirstLogIndex() {
        ensureOpen();
        this.readLock.lock();
        try {
            final AbstractFile firstFile = peekFirstAvailableFile();
            if (firstFile != null) {
                return firstFile.getFirstLogIndex();
            }
        } finally {
            this.readLock.unlock();
        }
        return -1L;
    }

    /**
     *
     * @return
     */
    public int getFirstLogPosition() {
        ensureOpen();
        this.readLock.lock();
        try {
            final AbstractFile firstFile = peekFirstAvailableFile();
            if (firstFile != null) {
                return firstFile.getFirstLogPosition();
            }
        } finally {
            this.readLock.unlock();
        }
        return -1;
    }

    private AbstractFile peekFirstAvailableFile() {
        for (AbstractFile file : this.files) {
            if (file.isAvailable()) {
                return file;
            }
        }
        return null;
    }

    private AbstractFile peekLastAvailableFile() {
        AbstractFile[] foreachFiles = new AbstractFile[this.files.size()];
        this.files.toArray(foreachFiles);
        for (int i = foreachFiles.length - 1; i >= 0; i--) {
            AbstractFile file = foreachFiles[i];
            if (file.isAvailable()) {
                return file;
            }
        }
        return null;
    }

    /**
     * get last log index
     *
     * @return -1L if nothing
     */
    public long getLastLogIndex() {
        ensureOpen();
        this.readLock.lock();
        try {
            final AbstractFile lastFile = peekLastAvailableFile();
            if (lastFile != null) {
                return lastFile.getLastLogIndex();
            }
        } finally {
            this.readLock.unlock();
        }
        return -1L;
    }

    /**
     * truncate logs from storage's head. We only deleted the expired file that
     * the last log index of file is less than firstIndexKept
     * we will return first log index of the store.
     * @param firstIndexKept
     * @return first log index of the store.
     * @throws IOException
     */
    public long truncatePrefix(final long firstIndexKept) throws IOException {
        ensureOpen();
        this.writeLock.lock();
        try {
            do {
                AbstractFile topFile = this.files.peekFirst();
                if (topFile == null || (topFile.isAvailable() && topFile.getLastLogIndex() >= firstIndexKept)) {
                    break;
                }
                deleteFile(topFile);
            } while (true);

        } finally {
            this.writeLock.unlock();
        }
        return this.getFirstLogIndex();
    }


    public boolean truncateSuffix(final long lastIndexKept) {
        return truncateSuffix(lastIndexKept, -1);
    }

    public boolean truncateSuffix(long lastIndexKept, final int lastLogPosition) {
        ensureOpen();
        this.writeLock.lock();
        try {
            if (this.getLastLogIndex() <= lastIndexKept) {
                return true;
            }
            final AbstractFile[] files = new AbstractFile[this.files.size()];
            for (int i = files.length - 1; i >= 0; i--) {
                final AbstractFile tailFile = files[i];
                if (tailFile.isAvailable() && tailFile.getLastLogIndex() <= lastIndexKept) {
                    return true;
                }
                if (tailFile.getFirstLogIndex() < lastIndexKept) {
                    // rest file
                    tailFile.restFile();
                } else {
                    // fill blank
                    int pos = tailFile.truncate(lastIndexKept, lastLogPosition);
                    if (pos > 0) {
                        this.setFlushedPosition(tailFile.getStartOffset() + pos);
                    }
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }


    public boolean deleteFile(final AbstractFile file) throws IOException {
        this.writeLock.lock();
        try {
            //Files are deleted from front to back, plz
            assert file == this.files.peekFirst();
            if (this.files.remove(file)) {
                this.directory.deleteFile(file.getFilename());
                return true;
            }
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    public void flush() throws IOException {
        this.writeLock.lock();
        try {
            if (this.files.isEmpty()) {
                return;
            }
            final AbstractFile[] fileArray = new AbstractFile[this.files.size()];
            this.files.toArray(fileArray);
            final long flushedPosition = this.getFlushedPosition();
            int i = fileArray.length - 1;
            for (; i >= 0; i--) {
                final AbstractFile file = fileArray[i];
                if (file.getStartOffset() <= flushedPosition) {
                    //find
                    break;
                }
            }
            long lastFlushedPosition = flushedPosition;
            if (i >= 0) {
                for (; i < fileArray.length; i++) {
                    final AbstractFile file = fileArray[i];
                    file.flush();
                    lastFlushedPosition = file.getStartOffset() + file.getFlushedPosition();
                }
            } else {
                final AbstractFile file = fileArray[0];
                file.flush();
                lastFlushedPosition = file.getStartOffset() + file.getFlushedPosition();
            }
            this.flushedPosition.set(lastFlushedPosition);
        } finally {
            this.writeLock.unlock();
        }
    }

    public long getFlushedPosition() {
        return this.flushedPosition.get();
    }

    void setFlushedPosition(final long flushedPosition) {
        this.flushedPosition.set(flushedPosition);
    }

    public boolean waitForFlush(final long maxExpectedFlushPosition, final int maxFlushTimes) throws IOException {
        int cnt = 0;
        long currentFlushedPosition;
        while ((currentFlushedPosition = getFlushedPosition()) < maxExpectedFlushPosition) {
            flush();
            cnt++;
            if (cnt > maxFlushTimes) {
                LOGGER.error("Try flush db {} times, but the flushPosition {} can't exceed expectedFlushPosition {}",
                        maxFlushTimes, currentFlushedPosition, maxExpectedFlushPosition);
                return false;
            }
        }
        return true;
    }


    private long calculateFileStartOffset() {
        if (this.files.isEmpty()) {
            return 0;
        }
        AbstractFile lastFile = this.files.peekLast();
        return calculateFileStartOffset(lastFile);
    }

    /**
     *
     */
    public void reset() {

    }

    @Override
    public String toString() {
        final StringBuilder infoBuilder = new StringBuilder(this.getClass().getSimpleName());
        infoBuilder.append("{");
        infoBuilder.append("directory=").append(this.directory.toString());
        infoBuilder.append(",").append("file_count=").append(this.files.size());
        infoBuilder.append(",").append("file_size=").append(this.fileSize);
        infoBuilder.append(",").append("next_file_sequence=").append(this.nextFileSequence.get());
        infoBuilder.append(",").append("closed=").append(this.closed);
        infoBuilder.append("}");
        return infoBuilder.toString();
    }

    static long calculateFileStartOffset(final AbstractFile prevFile) {
        if (prevFile == null) {
            return 0L;
        }
        return prevFile.getStartOffset() + prevFile.getFileSize();
    }
}
