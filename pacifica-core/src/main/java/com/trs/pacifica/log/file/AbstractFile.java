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

import com.trs.pacifica.log.dir.Directory;
import com.trs.pacifica.log.io.Input;
import com.trs.pacifica.log.io.Output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractFile {

    protected static final byte _FILE_END_BYTE = 'x';

    protected final FileHeader header = new FileHeader();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();


    private final AtomicInteger currentPosition = new AtomicInteger(0);
    private final AtomicInteger currentFlushPosition = new AtomicInteger(0);


    private final Directory parentDir;

    private final String filename;

    private final long fileSize;

    private long lastLogIndex;


    public AbstractFile(final Directory parentDir, final String filename) throws IOException {
        this.parentDir = parentDir;
        this.filename = filename;
        this.fileSize = parentDir.fileLength(filename);
        this.loadHeader();
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
        try {
            if (this.header.isBlank()) {
                this.header.setFirstLogIndex(logIndex);
                this.saveHeader();
            }
            int wrotePosition = this.currentPosition.get();
            writeBytes(data);
            // set last log index
            this.lastLogIndex = logIndex;
            return wrotePosition;
        } finally {
            this.writeLock.unlock();
        }
    }

    private void loadHeader() throws IOException {
        try (final Input input = this.parentDir.openInOutput(this.filename);) {
            byte[] bytes = new byte[FileHeader.getBytesSize()];
            input.seek(0);
            input.readBytes(bytes);
            this.header.decode(ByteBuffer.wrap(bytes));
        }
    }

    private void saveHeader() throws IOException {
        final ByteBuffer byteBuffer = this.header.encode();
        writeBytes(byteBuffer.array());
    }

    private void writeBytes(final byte[] bytes) throws IOException {
        assert bytes != null;
        assert bytes.length > 0;
        try (final Output output = this.parentDir.openInOutput(this.filename);) {
            output.writeBytes(bytes);
            this.currentPosition.getAndAdd(bytes.length);
        }
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
            return (int)(this.fileSize - this.currentPosition.get());
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
            if (this.currentPosition.get() >= this.fileSize) {
                return;
            }
            final int emptySize = (int) (this.fileSize - this.currentPosition.get());
            byte[] footer = new byte[emptySize];
            for (int i = 0; i < footer.length; i++) {
                footer[i] = _FILE_END_BYTE;
            }
            writeBytes(footer);
        } finally {
            this.writeLock.unlock();
        }
    }


    public void flush() {

    }

    public void close() {

    }


}
