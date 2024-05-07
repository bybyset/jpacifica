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
import com.trs.pacifica.util.io.*;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@NotThreadSafe
public abstract class AbstractFile implements Closeable {

    public static int _NOT_FOUND = Integer.MIN_VALUE;

    static int WRITE_BYTE_BUFFER_SIZE = 1024;
    protected static final byte _FILE_END_BYTE = 'x';
    protected final FileHeader header = new FileHeader();
    private final AtomicInteger currentPosition = new AtomicInteger(FileHeader.getBytesSize());
    private final AtomicInteger currentFlushPosition = new AtomicInteger(FileHeader.getBytesSize());
    protected final Directory parentDir;
    protected final String filename;
    protected final int fileSize;
    protected long lastLogIndex;


    public AbstractFile(final Directory parentDir, final String filename) throws IOException {
        this.parentDir = parentDir;
        this.filename = filename;
        this.fileSize = parentDir.fileLength(filename);
    }

    /**
     * load
     *
     * @throws IOException
     */
    public void load() throws IOException {
        this.loadHeader();
        this.loadBody();
    }

    protected abstract void loadBody() throws IOException;

    protected void loadHeader() throws IOException {
        try (final Input input = this.parentDir.openInOutput(this.filename);) {
            byte[] bytes = new byte[FileHeader.getBytesSize()];
            input.seek(0);
            input.readBytes(bytes);
            this.header.decode(bytes);
        }
    }

    /**
     * append data to
     *
     * @param logIndex  index of append log
     * @param dataInput bytes of append log
     * @return number of bytes written
     * @throws IOException
     */
    protected int doAppendData(final long logIndex, final DataBuffer dataInput) throws IOException {
        if (!this.header.isAvailable()) {
            this.header.setFirstLogIndex(logIndex);
            this.header.setAvailable();
            this.saveHeader();
        }
        final int writeBytes = appendData(dataInput);
        assert writeBytes == dataInput.remaining();
        // set last log index
        this.lastLogIndex = logIndex;
        return writeBytes;
    }

    /**
     * @param dataInput
     * @return number of bytes written
     * @throws IOException
     */
    protected int doAppendData(final DataBuffer dataInput) throws IOException {
        if (header.isBlank()) {
            //
            assert !header.isConsecutive();
            header.setConsecutive();
            this.saveHeader();
        }
        final int writeBytes = appendData(dataInput);
        assert writeBytes == dataInput.remaining();
        return writeBytes;
    }


    private void saveHeader() throws IOException {
        final byte[] headerData = this.header.encode();
        final int writeBytes = putData(0, new ByteDataBuffer(headerData));
        assert writeBytes == FileHeader._HEADER_BYTE_SIZE;
    }


    /**
     * @param index
     * @param data
     * @return number of bytes written
     * @throws IOException
     */
    private int putData(final int index, final DataBuffer data) throws IOException {
        final int position = this.currentPosition.get();
        if (index < 0 || index > position) {
            throw new IllegalArgumentException(String.format("index(%d) greater than position(%d) or index less than 0", index, position));
        }
        assert data != null;
        if (!data.hasRemaining()) {
            return 0;
        }

        int writeByteSize = 0;
        final byte[] buffer = new byte[WRITE_BYTE_BUFFER_SIZE];
        try (final Output output = this.parentDir.openInOutput(this.filename);) {
            int freeByteSize = this.fileSize - position;
            while (data.hasRemaining() && freeByteSize > 0) {
                int readLen = Math.min(buffer.length, data.limit() - data.position());
                data.get(buffer, 0, readLen);
                output.writeBytes(position + writeByteSize, buffer, 0, readLen);
                writeByteSize += readLen;
                freeByteSize -= readLen;
            }
        }
        return writeByteSize;
    }

    /**
     * @param data
     * @return number of bytes written
     * @throws IOException
     */
    protected int appendData(final DataBuffer data) throws IOException {
        final int currentPosition = this.currentPosition.get();
        final int writeByteSize = putData(currentPosition, data);
        this.currentPosition.getAndAdd(writeByteSize);
        return writeByteSize;
    }

    int readBytes(@Nonnull final byte[] bytes, final int position) throws IOException {
        return readBytes(bytes, position, bytes.length);
    }

    /**
     * Reads up to len bytes of data from the input stream into an array of bytes.
     * An attempt is made to read as many as len bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     * If len is zero, then no bytes are read and 0 is returned;
     * otherwise, there is an attempt to read at least one byte.
     * If no byte is available because the stream is at end of file, the value -1 is returned;
     * otherwise, at least one byte is read and stored into bytes.
     *
     * @param bytes
     * @param position
     * @param len
     * @return
     * @throws IOException
     */
    int readBytes(@Nonnull final byte[] bytes, final int position, final int len) throws IOException {
        Objects.checkFromIndexSize(0, len, bytes.length);
        if (len == 0) {
            return 0;
        }
        if (position < this.currentFlushPosition.get()) {
            return -1;
        }
        try (Input input = this.parentDir.openInOutput(this.filename)) {
            input.seek(position);
            input.readBytes(bytes, len);
        }
        return -1;
    }

    /**
     *
     * @param position
     * @param len
     * @return
     * @throws IOException
     */
    DataBuffer readDataBuffer(final int position, final int len) throws IOException {
        final int currentFlushPosition = this.currentFlushPosition.get();
        if (position < currentFlushPosition ) {
            throw new IndexOutOfBoundsException(String.format("position(%d) less than current_flush_position(%d).", position, currentFlushPosition));
        }
        if (position + len > this.fileSize) {
            throw new IndexOutOfBoundsException(String.format("position(%d)+len(%d) greater than file_size(%d)", position, len, fileSize));
        }
        if (len <= 0) {
            return new EmptyDataBuffer();
        }
        List<DataBuffer> dataBufferList = new ArrayList<>();
        try (Input input = this.parentDir.openInOutput(this.filename)) {
            input.seek(position);
            int readRemaining = len;
            while (readRemaining > 0) {
                int blockSize = Math.min(readRemaining, 1024);
                final byte[] bytes = new byte[blockSize];
                input.readBytes(bytes);
                readRemaining -= blockSize;
                dataBufferList.add(new ByteDataBuffer(bytes));
            }
        }
        return new LinkedDataBuffer(dataBufferList);
    }

    /**
     * @return
     */
    public long getFirstLogIndex() {
        return this.header.getFirstLogIndex();

    }

    public long getLastLogIndex() {
        return this.lastLogIndex;

    }

    public long getStartOffset() {
        return this.header.getStartOffset();
    }

    public long getFileSize() {
        return this.fileSize;
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
        return this.fileSize - this.currentPosition.get();
    }

    /**
     * fill bytes at the end of the file
     */
    public void fillEmptyBytesInFileEnd() throws IOException {
        if (this.currentPosition.get() >= this.fileSize) {
            return;
        }
        final int emptySize = getFreeByteSize();
        byte[] footer = new byte[emptySize];
        for (int i = 0; i < footer.length; i++) {
            footer[i] = _FILE_END_BYTE;
        }
        appendData(new ByteDataBuffer(footer));
    }

    public String getFilename() {
        return filename;
    }


    public boolean isAvailable() {
        return this.header.isAvailable();
    }

    public void restFile(int position) {
        this.rest(position);
        if (position == 0) {
            this.restHeader();
        }
    }

    private void rest(final int position) {
        this.currentPosition.set(position);
        this.currentFlushPosition.set(position);
    }

    private void restHeader() {
        this.header.rest();
        try (Output output = this.parentDir.openInOutput(this.filename)) {

        } catch (IOException e) {

        }
    }

    @Override
    public void close() throws IOException {

    }
}
