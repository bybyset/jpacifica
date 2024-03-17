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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractStore {


    static final String _FILENAME_FLUSH_CHECKPOINT = "_flush.checkpoint";

    /**
     * Used to name new file.
     */
    private final AtomicInteger nextFileSequence = new AtomicInteger(0);

    private final Deque<AbstractFile> files = new ArrayDeque<>();

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    protected final Lock readLock = lock.readLock();

    protected final Lock writeLock = lock.writeLock();


    protected final BaseDirectory directory;


    public AbstractStore(Path dir) throws IOException {
        this.directory = FsDirectory.open(dir);
    }


    private final String getNextFilename() {
        return String.format("%019d", this.nextFileSequence.getAndIncrement()) + getFileSuffix();
    }

    /**
     * get file suffix when create next file
     * @return
     */
    protected abstract String getFileSuffix();


    protected abstract AbstractFile doAllocateFile(final String filename) throws IOException;


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
     * get the file of last write
     * @param minFreeByteSize    minimum number of free bytes of a file
     * @param createIfNecessary  create
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
                final AbstractFile file = fileArray[mid];
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


    public boolean deleteFile(final AbstractFile file) throws IOException{
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

}
