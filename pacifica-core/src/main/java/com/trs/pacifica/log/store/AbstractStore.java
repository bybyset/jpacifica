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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
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


}
