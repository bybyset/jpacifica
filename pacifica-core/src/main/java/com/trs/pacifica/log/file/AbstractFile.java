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

package com.trs.pacifica.log.file;

import com.trs.pacifica.log.io.Output;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractFile implements Output {

    protected static final byte _FILE_END_BYTE = 'x';

    protected final FileHeader header = new FileHeader();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();


    private final AtomicInteger currentPosition = new AtomicInteger(0);
    private final AtomicInteger currentFlushPosition = new AtomicInteger(0);


    private final String filePath;

    private int fileSize;

    private long lastLogIndex;


    public AbstractFile(final String filePath, final int fileSize, final long firstLogIndex, final long startOffset) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.header.setFirstLogIndex(firstLogIndex);
        this.header.setStartOffset(startOffset);
    }


    public String getFilePath() {
        return this.filePath;
    }


    public void open() throws IOException {
        this.writeLock.lock();
        try {


            loadHeader();
        } finally {
            this.writeLock.unlock();
        }
    }


    /**
     * append data to
     *
     * @param logIndex index of append log
     * @param data     bytes of append log
     * @return start wrote position
     * @throws IOException
     */
    protected int doAppendData(final long logIndex, final byte[] data) throws IOException {
        this.writeLock.lock();
        int wrotePosition = this.currentPosition.get();
        try {
            if (this.header.isBlank()) {
                this.header.setFirstLogIndex(logIndex);
                this.saveHeader();
                wrotePosition = this.header.getBytesSize();
            }
            writeBytes(data);
            // set current position write
            this.currentPosition.set(wrotePosition + data.length);
            // set last log index
            this.lastLogIndex = logIndex;
            return wrotePosition;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void writeByte(byte b) throws IOException {

    }


    private void loadHeader() {

    }

    private void saveHeader() {

    }


    /**
     * @param index
     * @param data
     * @return
     */
    protected int appendBytes(final long index, final byte[] data) {

        return 0;
    }

    /**
     * @return
     */
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            return this.header.getFirstLogIndex();
        } finally {
            this.readLock.unlock();
        }

    }

    public long getLastLogIndex() {
        this.readLock.lock();
        try {
            return this.lastLogIndex;
        } finally {
            this.readLock.unlock();
        }

    }

    public long getStartOffset() {
        this.readLock.lock();
        try {
            return this.header.getStartOffset();
        } finally {
            this.readLock.unlock();
        }
    }

    public long getFileSize() {
        this.readLock.lock();
        try {
            return this.fileSize;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * get current write position in the file
     *
     * @return
     */
    public int getCurrentPosition() {
        return this.currentPosition.get();
    }

    /**
     * get current flushed position in the file
     *
     * @return
     */
    public int getCurrentFlushPosition() {
        return this.currentFlushPosition.get();
    }


    /**
     * Get the free available byte space
     *
     * @return
     */
    public int getFreeByteSize() {
        this.readLock.lock();
        try {
            return this.fileSize - this.currentPosition.get();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * fill bytes at the end of the file
     */
    public void fillEmptyBytesInFileEnd() throws IOException {
        this.writeLock.lock();
        try {
            if (this.currentPosition.get() == this.fileSize) {
                return;
            }
            while (this.currentPosition.getAndIncrement() < this.fileSize) {
                writeByte(_FILE_END_BYTE);
            }
        } finally {
            this.writeLock.unlock();
        }
    }


    public void flush() {

    }

    public void close() {

    }


}
