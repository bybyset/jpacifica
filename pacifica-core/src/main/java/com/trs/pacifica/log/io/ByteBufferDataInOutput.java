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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@NotThreadSafe
public class ByteBufferDataInOutput implements DataInOutput {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    protected final ByteBuffer[] byteBuffers;

    private final long length;

    private final int chunkSizePower;

    protected final long chunkSizeMask;

    private final Context outputContext = new Context();

    private final Context inputContext = new Context();


    public ByteBufferDataInOutput(final ByteBuffer[] byteBuffers, final int chunkSizePower, final long length) {
        this.byteBuffers = byteBuffers;
        this.chunkSizePower = chunkSizePower;
        this.chunkSizeMask = (1L << chunkSizePower) - 1L;
        this.length = length;
    }

    @Override
    public void seek(long pos) throws IOException {
        final int bi = (int) (pos >> chunkSizePower);
        if (bi == this.inputContext.curByteBufferIndex) {
            this.inputContext.curByteBuffer.position((int) (pos & chunkSizeMask));
        } else {
            final ByteBuffer b = byteBuffers[bi];
            b.position((int) (pos & chunkSizeMask));
            this.inputContext.curByteBufferIndex = bi;
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (!this.inputContext.curByteBuffer.hasRemaining()) {
            do {
                int currentIndex = this.inputContext.curByteBufferIndex + 1;
                if (currentIndex >= byteBuffers.length) {
                    throw new EOFException("read past EOF: " + this);
                }
                setCurByteBuffer(this.inputContext, currentIndex);
                this.inputContext.curByteBuffer.position(0);
            } while (!this.inputContext.curByteBuffer.hasRemaining());
        }
        return this.inputContext.curByteBuffer.get();
    }


    @Override
    public void writeByte(byte b) throws IOException {
        if (!this.outputContext.curByteBuffer.hasRemaining()) {
            do {
                int currentIndex = this.outputContext.curByteBufferIndex + 1;
                if (currentIndex >= byteBuffers.length) {
                    throw new EOFException("write past EOF: " + this);
                }
                setCurByteBuffer(this.outputContext, currentIndex);
            } while (!this.inputContext.curByteBuffer.hasRemaining());
        }
        this.outputContext.curByteBuffer.put(b);
    }

    public long getWritePosition() {
        return calculatePosition(this.outputContext);
    }

    public long getReadPosition() {
        return calculatePosition(this.inputContext);
    }

    private void setCurByteBuffer(final Context context, int byteBufferIndex) {
        assert byteBufferIndex < this.byteBuffers.length;
        ByteBuffer byteBuffer = this.byteBuffers[byteBufferIndex].duplicate();
        setContext(context, byteBuffer, byteBufferIndex);
    }

    long calculatePosition(Context context) {
        return (((long) context.curByteBufferIndex) << chunkSizePower) + context.curByteBuffer.position();
    }

    static void setContext(final Context context, ByteBuffer byteBuffer, int byteBufferIndex) {
        context.curByteBuffer = byteBuffer;
        context.curByteBufferIndex = byteBufferIndex;
    }


    static class Context {

        protected ByteBuffer curByteBuffer = EMPTY;

        protected int curByteBufferIndex = -1;

    }


}
