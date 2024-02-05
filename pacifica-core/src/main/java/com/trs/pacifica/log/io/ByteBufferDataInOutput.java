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

import com.trs.pacifica.log.error.AlreadyClosedException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@NotThreadSafe
public class ByteBufferDataInOutput implements InOutput {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    private final String resourceDescription;
    protected final ByteBufferGuard guard;

    protected final ByteBuffer[] byteBuffers;

    private final long length;

    private final int chunkSizePower;

    protected final long chunkSizeMask;

    private final Context outputContext = new Context();

    private final Context inputContext = new Context();

    protected boolean isClone = false;

    public ByteBufferDataInOutput(String resourceDescription, final ByteBuffer[] byteBuffers, final int chunkSizePower, final long length, ByteBufferGuard guard) {
        this.resourceDescription = resourceDescription;
        this.byteBuffers = byteBuffers;
        this.chunkSizePower = chunkSizePower;
        this.chunkSizeMask = (1L << chunkSizePower) - 1L;
        this.length = length;
        this.guard = guard;
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

    @Override
    public void flush() throws IOException {

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

    @Override
    public InOutput slice(String sliceDescription, long offset, long length) throws IOException {
        if (byteBuffers == null) {
            throw alreadyClosed(null);
        }

        final ByteBuffer[] newBuffers = buildSlice(byteBuffers, offset, length);
        final int ofs = (int) (offset & chunkSizeMask);

        final ByteBufferDataInOutput clone =
                newCloneInstance(getFullSliceDescription(sliceDescription), newBuffers, ofs, length);
        clone.isClone = true;

        return clone;
    }

    @Override
    public InOutput clone() {
        final ByteBufferDataInOutput clone;
        try {
            clone = (ByteBufferDataInOutput) slice(this.resourceDescription, 0L, this.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return clone;
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } catch (IOException e) {
            //ignore
        }
        this.guard.invalidateAndUnmap(byteBuffers);
    }

    /**
     * Returns a sliced view from a set of already-existing buffers: the last buffer's limit() will be
     * correct, but you must deal with offset separately (the first buffer will not be adjusted)
     */
    private ByteBuffer[] buildSlice(ByteBuffer[] buffers, long offset, long length) {
        final long sliceEnd = offset + length;

        final int startIndex = (int) (offset >>> chunkSizePower);
        final int endIndex = (int) (sliceEnd >>> chunkSizePower);

        // we always allocate one more slice, the last one may be a 0 byte one
        final ByteBuffer[] slices = new ByteBuffer[endIndex - startIndex + 1];

        for (int i = 0; i < slices.length; i++) {
            slices[i] = buffers[startIndex + i].duplicate().order(ByteOrder.LITTLE_ENDIAN);
        }

        // set the last buffer's limit for the sliced view.
        slices[slices.length - 1].limit((int) (sliceEnd & chunkSizeMask));

        return slices;
    }

    /**
     * Factory method that creates a suitable implementation of this class for the given ByteBuffers.
     */
    @SuppressWarnings("resource")
    protected ByteBufferDataInOutput newCloneInstance(
            String newResourceDescription, ByteBuffer[] newBuffers, int offset, long length) {
        return ByteBufferDataInOutput.newInstance(newResourceDescription, newBuffers, length, chunkSizePower, guard);
    }

    /**
     * Subclasses call this to get the String for resourceDescription of a slice of this {@code
     * IndexInput}.
     */
    protected String getFullSliceDescription(String sliceDescription) {
        if (sliceDescription == null) {
            // Clones pass null sliceDescription:
            return toString();
        } else {
            return toString() + " [slice=" + sliceDescription + "]";
        }
    }

    AlreadyClosedException alreadyClosed(RuntimeException unused) {
        return new AlreadyClosedException("Already closed: " + this);
    }


    public static ByteBufferDataInOutput newInstance(
            String resourceDescription,
            ByteBuffer[] buffers,
            long length,
            int chunkSizePower,
            ByteBufferGuard guard) {
        return new ByteBufferDataInOutput(resourceDescription, buffers, chunkSizePower, length, guard);
    }

    static class Context {

        protected ByteBuffer curByteBuffer = EMPTY;

        protected int curByteBufferIndex = -1;

    }


}
