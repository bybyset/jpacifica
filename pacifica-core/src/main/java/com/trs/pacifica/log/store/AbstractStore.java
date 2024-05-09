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

import com.trs.pacifica.log.dir.BaseDirectory;
import com.trs.pacifica.log.dir.FsDirectory;
import com.trs.pacifica.log.file.AbstractFile;
import com.trs.pacifica.model.LogId;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStore.class);

    static final String _FILENAME_FLUSH_CHECKPOINT = "_flush.checkpoint";

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


    protected AbstractStore(Path dir, int fileSize) throws IOException {
        this.directory = FsDirectory.open(dir);
        this.fileSize = fileSize;
        load();
    }


    protected void load() throws IOException {
        loadExistedFiles();
        if (this.files.isEmpty()) {

        }

        // set flushed position

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
        Arrays.sort(filenames, Comparator.comparing(this::getFileSequenceFromFilename));
        AbstractFile lastFile = null;
        long nextFileSequence = 0;
        for (String filename : filenames) {
            final AbstractFile file = loadFile(filename, lastFile);
            if (file == null) {
                continue;
            }
            lastFile = file;
            this.files.add(file);
            nextFileSequence = Math.max(nextFileSequence, getFileSequenceFromFilename(filename) + 1);
        }
        this.nextFileSequence.set(nextFileSequence);
    }


    protected AbstractFile loadFile(final String filename, final AbstractFile lastFile) throws IOException {
        if (filename != null && filename.endsWith(getFileSuffix())) {
            AbstractFile abstractFile = newAbstractFile(filename);
            if (!abstractFile.load()) {
                abstractFile.setStartOffset(calculateFileStartOffset(lastFile));
            }
            return abstractFile;
        }
        return null;
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


    /**
     * get first log index
     *
     * @return -1L if nothing
     */
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            final AbstractFile firstFile = this.files.peekFirst();
            if (firstFile != null) {
                return firstFile.getFirstLogIndex();
            }
        } finally {
            this.readLock.unlock();
        }
        return -1L;
    }

    /**
     * get last log index
     *
     * @return -1L if nothing
     */
    public long getLastLogIndex() {
        this.readLock.lock();
        try {
            final AbstractFile lastFile = this.files.peekLast();
            if (lastFile != null) {
                return lastFile.getLastLogIndex();
            }
        } finally {
            this.readLock.unlock();
        }
        return -1L;
    }

    public boolean truncatePrefix(final long firstIndexKept) throws IOException {
        this.writeLock.lock();
        try {
            do {
                AbstractFile topFile = this.files.peekFirst();
                if (topFile == null || topFile.getLastLogIndex() >= firstIndexKept) {
                    return true;
                }
                deleteFile(topFile);
            } while (true);

        } finally {
            this.writeLock.unlock();
        }
    }

    public boolean truncateSuffix(long lastIndexKept) {
        this.writeLock.lock();
        try {
            if (this.getLastLogIndex() <= lastIndexKept) {
                return true;
            }
            do {
                AbstractFile tailFile = this.files.peekLast();
                if (tailFile == null || tailFile.getLastLogIndex() <= lastIndexKept) {
                    return true;
                }
                if (tailFile.getFirstLogIndex() < lastIndexKept) {
                    // rest file at 0
                    tailFile.restFile(0);
                } else {
                    // rest file at position of lastIndexKept

                }

            } while (true);
        } finally {
            this.writeLock.unlock();
        }
    }


    public boolean deleteFile(final AbstractFile file) throws IOException {
        this.writeLock.lock();
        try {
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

    static long calculateFileStartOffset(final AbstractFile lastFile) {
        if (lastFile == null) {
            return 0L;
        }
        return lastFile.getStartOffset() + lastFile.getFileSize();
    }
}
