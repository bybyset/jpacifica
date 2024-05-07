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
        this.limit(totalLimit);
        this.totalCapacity = totalCapacity;
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

        if (!this.blocks[blockIndex].dataBuffer.hasRemaining()) {
            blockIndex++;
        }
        position(++pos);
        return this.blocks[blockIndex].dataBuffer.get();
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
    public DataBuffer get(byte[] dst, final int offset, int length) {
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
                if (remaining < length) {
                    blocks[blockIndex++].dataBuffer.get(dst, tmpOffset, remaining);
                    tmpOffset += remaining;
                } else {
                    blocks[blockIndex].dataBuffer.get(dst, tmpOffset, length);
                    tmpOffset += length;
                }
            } else {
                blockIndex++;
            }
        } while (tmpOffset - offset < length);
        return this;
    }

    @Override
    public DataBuffer slice() {
        throw new NotSupportedException("not support slice");
    }

    @Override
    public DataBuffer slice(int index, int length) {
        throw new NotSupportedException("not support slice");
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
