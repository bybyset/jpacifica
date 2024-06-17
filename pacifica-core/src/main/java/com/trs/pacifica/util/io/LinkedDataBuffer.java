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

package com.trs.pacifica.util.io;

import com.trs.pacifica.error.NotSupportedException;

import java.nio.BufferUnderflowException;
import java.util.*;

public class LinkedDataBuffer extends AbstractDataBuffer{

    private final Block[] blocks;

    private final int totalCapacity;

    private int blockIndex;

    private int lastGetPosition = -1;

    public LinkedDataBuffer(List<DataBuffer> dataBuffers) {
        this.blockIndex = 0;
        this.blocks = new Block[dataBuffers.size()];
        int totalLimit = 0;
        int totalCapacity = 0;
        for (int i = 0; i < dataBuffers.size(); i++) {
            DataBuffer dataBuffer = dataBuffers.get(i);
            if (dataBuffer.position() != 0) {
                throw new IllegalArgumentException("position is not 0");
            }
            this.blocks[i] = new Block(totalLimit, dataBuffer);
            totalLimit += dataBuffer.limit();
            totalCapacity += dataBuffer.capacity();
        }
        this.totalCapacity = totalCapacity;
        this.limit(totalLimit);
    }


    public LinkedDataBuffer(DataBuffer... dataBuffers) {
        this(List.of(dataBuffers));
    }

    @Override
    public int capacity() {
        return totalCapacity;
    }

    @Override
    public byte get() {
        int pos = position();
        if (pos >= limit()) {
            throw new BufferUnderflowException();
        }
        Block block = null;
        if (this.lastGetPosition != pos) {
            block = findDataBuffer(pos);
            int blockPos = pos - block.startIndex;
            block.dataBuffer.position(blockPos);
        } else {
            block = this.blocks[blockIndex];
        }
        if (block == null) {
            throw new BufferUnderflowException();
        }
        if (!block.dataBuffer.hasRemaining()) {
            blockIndex++;
        }
        this.lastGetPosition = pos;
        position(++pos);
        return block.dataBuffer.get();
    }

    @Override
    public byte get(int index) {
        if (index < 0 || index > limit()) {
            throw new IndexOutOfBoundsException(String.format("index(%d) is less than 0 or greater than limit(%d).", index, limit()));
        }
        final Block find = findDataBuffer(index);
        assert find != null;
        int interIndex = index - find.startIndex;
        return find.dataBuffer.get(interIndex);
    }

    @Override
    public DataBuffer get(byte[] dst, final int offset, final int length) {
        Objects.checkFromIndexSize(offset, length, dst.length);
        int pos = position();
        if (length > limit() - pos)
            throw new BufferUnderflowException();
        int tmpOffset = offset;
        do {
            if (blockIndex >= blocks.length) {
                break;
            }
            int remaining = blocks[blockIndex].dataBuffer.remaining();
            if (remaining > 0) {
                int readLen = offset + length - tmpOffset;
                if (remaining < readLen) {
                    blocks[blockIndex++].dataBuffer.get(dst, tmpOffset, remaining);
                    tmpOffset += remaining;
                } else {
                    blocks[blockIndex].dataBuffer.get(dst, tmpOffset, readLen);
                    tmpOffset += readLen;
                }
            } else {
                blockIndex++;
            }
        } while (tmpOffset - offset < length);
        this.position(pos + tmpOffset - offset);
        return this;
    }

    @Override
    public DataBuffer slice() {
        DataBuffer[] dataBuffers = new DataBuffer[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            dataBuffers[i] = blocks[i].dataBuffer.slice();
        }
        return new LinkedDataBuffer(dataBuffers);
    }

    @Override
    public DataBuffer slice(int index, int length) {
        if (index >= this.limit()) {
            throw new IndexOutOfBoundsException(String.format("index(%d) greater than limit(%d).", index, limit()));
        }
        if (length <= 0) {
            return new EmptyDataBuffer();
        }
        List<DataBuffer> dataBuffers = new ArrayList<>(blocks.length);
        int i = 0;
        for (; i < blocks.length;) {
            DataBuffer dataBuffer = blocks[i].dataBuffer;
            if (index >= blocks[i].startIndex && index < blocks[i].startIndex + dataBuffer.limit()) {
                int sliceIndex = Math.max(0, index - blocks[i].startIndex);
                int sliceLen = Math.min(length, dataBuffer.limit() - sliceIndex);
                dataBuffer = dataBuffer.slice(sliceIndex, sliceLen);
                dataBuffers.add(dataBuffer);
                length -= sliceLen;
                i++;
                break;
            }
            i++;
        }
        for (; i < blocks.length && length > 0; i++) {
            DataBuffer dataBuffer = blocks[i].dataBuffer;
            int sliceLen = Math.min(length, dataBuffer.limit());
            dataBuffer = dataBuffer.slice(0, sliceLen);
            dataBuffers.add(dataBuffer);
            length -= sliceLen;
        }
        return new LinkedDataBuffer(dataBuffers);
    }

    private Block findDataBuffer(int index) {
        for (int i = 0; i < blocks.length; i++) {
            int endIndex = blocks[i].startIndex + blocks[i].dataBuffer.limit();
            if (blocks[i].startIndex <= index && index < endIndex) {
                return blocks[i];
            }
        }
        return null;
    }



    private class Block {
        private final int startIndex;

        private final DataBuffer dataBuffer;

        private Block(int startIndex, DataBuffer dataBuffer) {
            this.startIndex = startIndex;
            this.dataBuffer = dataBuffer;
        }
    }
}
