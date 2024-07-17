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

package com.trs.pacifica.log.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

public class ByteBufferDataInOutputTest {

    static final long DEFAULT_MAX_CHUNK_SIZE = 1L << 10; // 1k
    static final long DEFAULT_FILE_LENGTH = 10l << 10;// 10k

    ByteBufferGuard guard = Mockito.mock(ByteBufferGuard.class);
    long length = DEFAULT_FILE_LENGTH;
    int chunkSizePower = chunkSizePower(DEFAULT_MAX_CHUNK_SIZE);
    long chunkSizeMask = (1L << chunkSizePower) - 1L;
    ByteBuffer[] byteBuffers = mockByteBuffers(length, chunkSizePower);

    @InjectMocks
    ByteBufferDataInOutput byteBufferDataInOutput;

    @BeforeEach
    public void setUp() {
        byteBufferDataInOutput = new ByteBufferDataInOutput("test", byteBuffers, chunkSizePower, length, guard);
    }


    public void mockInit(long fileLength, long chunkSizePower) {
        long chunkSizeMask = (1L << chunkSizePower) - 1L;
        final ByteBuffer[] byteBuffers = mockByteBuffers(fileLength, chunkSizePower);
        Mockito.mock("length", fileLength);
        Mockito.mock("chunkSizePower", chunkSizePower);
        Mockito.mock("chunkSizeMask", chunkSizeMask);
        Mockito.mock("byteBuffers", byteBuffers);
    }

    public void mockInit() {
        mockInit(DEFAULT_FILE_LENGTH, chunkSizePower(DEFAULT_MAX_CHUNK_SIZE));
    }

    @Test
    public void testSeek() throws IOException {

        byteBufferDataInOutput.seek(0L);
        ByteBufferDataInOutput.Context context = byteBufferDataInOutput.getInputContext();
        Assertions.assertEquals(0, context.curByteBufferIndex);
        Assertions.assertNotNull(context.curByteBuffer);
        Assertions.assertEquals(context.curByteBuffer.position(), 0);
        Assertions.assertEquals(byteBuffers[0], context.curByteBuffer);

        byteBufferDataInOutput.seek(chunkSizeMask + 1);
        context = byteBufferDataInOutput.getInputContext();
        Assertions.assertEquals(1, context.curByteBufferIndex);
        Assertions.assertNotNull(context.curByteBuffer);
        Assertions.assertEquals(context.curByteBuffer.position(), 0);
        Assertions.assertEquals(byteBuffers[1], context.curByteBuffer);

        byteBufferDataInOutput.seek(chunkSizeMask + 2);
        context = byteBufferDataInOutput.getInputContext();
        Assertions.assertEquals(1, context.curByteBufferIndex);
        Assertions.assertNotNull(context.curByteBuffer);
        Assertions.assertEquals(context.curByteBuffer.position(), 1);
        Assertions.assertEquals(byteBuffers[1], context.curByteBuffer);


        byteBufferDataInOutput.seek(length - 1);
        context = byteBufferDataInOutput.getInputContext();
        Assertions.assertEquals(byteBuffers.length - 1, context.curByteBufferIndex);
        Assertions.assertNotNull(context.curByteBuffer);
        Assertions.assertEquals(byteBuffers[byteBuffers.length - 1], context.curByteBuffer);


        try {
            byteBufferDataInOutput.seek(length);
        } catch (Throwable e) {
            Assertions.assertInstanceOf(EOFException.class, e);
        }

    }

    @Test
    void testReadByte() throws IOException {
        //
        byte[] testBytes = "test".getBytes("UTF-8");
        int len = testBytes.length;
        ByteBuffer buffer = this.byteBuffers[0];
        buffer = buffer.duplicate();
        buffer.put(testBytes);

        this.byteBufferDataInOutput.seek(0);
        byte[] readBytes = new byte[len];
        for(int i = 0; i < len; i++) {
            readBytes[i] = this.byteBufferDataInOutput.readByte();
        }
        Assertions.assertArrayEquals(testBytes, readBytes);
    }

    @Test
    void testReadBytes() throws IOException {
        //
        byte[] testBytes = "test".getBytes("UTF-8");
        int len = testBytes.length;
        ByteBuffer buffer = this.byteBuffers[0];
        buffer = buffer.duplicate();
        buffer.put(testBytes);
        this.byteBufferDataInOutput.seek(0);
        byte[] readBytes = new byte[len];
        int readLen = this.byteBufferDataInOutput.readBytes(readBytes, 0, readBytes.length);
        Assertions.assertEquals(len, readLen);
        Assertions.assertArrayEquals(testBytes, readBytes);

    }

    @Test
    void testReadBytes1() throws IOException {
        //
        byte[] helloBytes = "hello".getBytes("UTF-8");
        byte[] testBytes = "test".getBytes("UTF-8");
        int len = testBytes.length;
        ByteBuffer buffer = this.byteBuffers[1];
        buffer = buffer.duplicate();
        buffer.put(helloBytes);
        buffer.put(testBytes);
        this.byteBufferDataInOutput.seek(DEFAULT_MAX_CHUNK_SIZE + helloBytes.length);
        byte[] readBytes = new byte[len];
        int readLen = this.byteBufferDataInOutput.readBytes(readBytes, 0, readBytes.length);
        Assertions.assertEquals(len, readLen);
        Assertions.assertArrayEquals(testBytes, readBytes);

    }

    @Test
    void testReadBytes2() throws IOException {
        //
        byte[] helloBytes = "hello".getBytes("UTF-8");
        byte[] testBytes = "test".getBytes("UTF-8");
        ByteBuffer buffer = this.byteBuffers[0];
        buffer = buffer.duplicate();
        buffer.position((int) DEFAULT_MAX_CHUNK_SIZE - helloBytes.length);
        buffer.put(helloBytes);
        buffer = this.byteBuffers[1];
        buffer = buffer.duplicate();
        buffer.put(testBytes);

        this.byteBufferDataInOutput.seek(DEFAULT_MAX_CHUNK_SIZE - helloBytes.length);
        int len = helloBytes.length + testBytes.length;
        byte[] readBytes = new byte[len];
        int readLen = this.byteBufferDataInOutput.readBytes(readBytes, 0, readBytes.length);
        Assertions.assertEquals(len, readLen);
        byte[] expected = new byte[len];
        System.arraycopy(helloBytes, 0, expected, 0, helloBytes.length);
        System.arraycopy(testBytes, 0, expected, helloBytes.length, testBytes.length);
        Assertions.assertArrayEquals(expected, readBytes);
    }

    @Test
    void testWriteBytes() throws IOException {
        byte[] writeBytes = "test".getBytes("UTF-8");

        this.byteBufferDataInOutput.writeBytes(2, writeBytes, 0, writeBytes.length);
        ByteBuffer buffer = this.byteBuffers[0];
        buffer = buffer.duplicate();
        buffer.position(2);
        byte[] testBytes = new byte[writeBytes.length];
        buffer.get(testBytes);

        Assertions.assertArrayEquals(testBytes, writeBytes);
    }


    @Test
    void testSlice() throws IOException {
        ByteBufferDataInOutput clone = (ByteBufferDataInOutput) this.byteBufferDataInOutput.slice("test_slice", 0, (long)this.length);
        Assertions.assertEquals(this.length, clone.getLength());
        Assertions.assertEquals(this.byteBuffers.length, clone.byteBuffers.length);
        clone.close();
        Mockito.verify(guard, times(0)).invalidateAndUnmap(any());
    }

    @Test
    void testSlice0() throws IOException {
        ByteBufferDataInOutput clone = (ByteBufferDataInOutput) this.byteBufferDataInOutput.slice("test_slice", 0, DEFAULT_MAX_CHUNK_SIZE - 1);
        Assertions.assertEquals(DEFAULT_MAX_CHUNK_SIZE - 1, clone.getLength());
        Assertions.assertEquals(1, clone.byteBuffers.length);

        clone.close();
        Mockito.verify(guard, times(0)).invalidateAndUnmap(any());
    }

    @Test
    void testClone() throws IOException {
        ByteBufferDataInOutput clone = (ByteBufferDataInOutput) this.byteBufferDataInOutput.clone();
        Assertions.assertEquals(this.length, clone.getLength());
        Assertions.assertEquals(this.byteBuffers.length, clone.byteBuffers.length);
        clone.close();
        Mockito.verify(guard, times(0)).invalidateAndUnmap(any());
    }

    @Test
    void testClose() throws IOException {
        this.byteBufferDataInOutput.close();
        Mockito.verify(guard, times(1)).invalidateAndUnmap(this.byteBuffers);
    }

    static int chunkSizePower(long maxChunkSize) {
        return Long.SIZE - 1 - Long.numberOfLeadingZeros(maxChunkSize);
    }

    static ByteBuffer[] mockByteBuffers(long length, long chunkSizePower) {
        final long chunkSize = 1L << chunkSizePower;
        final int nrBuffers = (int) (length >>> chunkSizePower);
        final ByteBuffer[] buffers = new ByteBuffer[nrBuffers];
        long startOffset = 0L;
        for (int bufNr = 0; bufNr < nrBuffers; bufNr++) {
            final int bufSize =
                    (int) ((length > (startOffset + chunkSize)) ? chunkSize : (length - startOffset));
            ByteBuffer buffer = ByteBuffer.allocate(bufSize);
            buffers[bufNr] = buffer;
            startOffset += bufSize;
        }
        return buffers;
    }
}