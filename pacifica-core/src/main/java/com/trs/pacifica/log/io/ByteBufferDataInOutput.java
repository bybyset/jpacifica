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

import com.trs.pacifica.error.AlreadyClosedException;
import com.trs.pacifica.util.OnlyForTest;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

@NotThreadSafe
public class ByteBufferDataInOutput implements InOutput {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    private final String resourceDescription;
    protected final ByteBufferGuard guard;
    protected final ByteBuffer[] byteBuffers;
    private final long length;
    private final int chunkSizePower;
    protected final long chunkSizeMask;
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
        try {
            if (bi == this.inputContext.curByteBufferIndex) {
                this.inputContext.curByteBuffer.position((int) (pos & chunkSizeMask));
            } else {
                final ByteBuffer b = byteBuffers[bi];
                b.position((int) (pos & chunkSizeMask));
                setCurByteBuffer(this.inputContext, bi);
            }
        } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException e) {
            throw handlePositionalIOOBE(e, "seek", pos);
        } catch (NullPointerException e) {
            throw alreadyClosed(e);
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
    public void writeBytes(int index, byte[] bytes, int offset, int length) throws IOException {
        //check
        Objects.checkFromIndexSize(index, length, (int)this.length);
        Objects.checkFromIndexSize(offset, length, bytes.length);
        //seek
        int bi = (int) (index >> chunkSizePower);
        int position = (int) (index & chunkSizeMask);
        do {
            final ByteBuffer b = byteBuffers[bi];
            b.position(position);
            int len = Math.min(b.remaining(), length);
            b.put(bytes, offset, len);
            offset += len;
            length -= len;
            //next buffer
            bi++;
            position = 0;
        } while (length > 0);
    }

    public long getReadPosition() {
        return calculatePosition(this.inputContext);
    }

    private void setCurByteBuffer(final Context context, int byteBufferIndex) {
        assert byteBufferIndex < this.byteBuffers.length;
        ByteBuffer byteBuffer = this.byteBuffers[byteBufferIndex];
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
            clone.isClone = true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return clone;
    }

    @Override
    public void close() throws IOException {
        if (isClone) return;
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
        final ByteBuffer[] slices = new ByteBuffer[endIndex - startIndex];
        for (int i = 0; i < slices.length; i++) {
            slices[i] = buffers[startIndex + i].duplicate().order(ByteOrder.LITTLE_ENDIAN);
        }
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

    // the unused parameter is just to silence javac about unused variables
    RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos)
            throws IOException {
        if (pos < 0L) {
            return new IllegalArgumentException(action + " negative position (pos=" + pos + "): " + this);
        } else {
            throw new EOFException(action + " past EOF (pos=" + pos + "): " + this);
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

    @OnlyForTest
    long getLength(){
        return this.length;
    }

    @OnlyForTest
    Context getInputContext() {
        return this.inputContext;
    }

    static class Context {

        protected ByteBuffer curByteBuffer = EMPTY;

        protected int curByteBufferIndex = -1;

    }


}
