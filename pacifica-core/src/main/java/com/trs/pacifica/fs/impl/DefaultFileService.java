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

package com.trs.pacifica.fs.impl;

import com.google.protobuf.ByteString;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.fs.FileReader;
import com.trs.pacifica.fs.FileService;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultFileService implements FileService {
    static final Logger LOGGER = LoggerFactory.getLogger(FileService.class);

    private final AtomicLong readerIdAllocator = new AtomicLong();

    private final Map<Long, FileReader> fileReaderMap = new ConcurrentHashMap<>();

    @Override
    public FileReader getFileReader(long readerId) {
        return this.fileReaderMap.get(readerId);
    }

    @Override
    public long addFileReader(FileReader fileReader) {
        final long readId = allocateReaderId();
        this.fileReaderMap.put(readId, fileReader);
        return readId;
    }

    @Override
    public boolean removeFileReader(long readerId) {
        return false;
    }

    @Override
    public RpcRequest.GetFileResponse handleGetFileRequest(RpcRequest.GetFileRequest request, RpcRequestFinished<RpcRequest.GetFileResponse> callback) {

        try {
            if (request.getOffset() < 0 || request.getLength() <= 0) {
                throw new PacificaException(PacificaErrorCode.UNAVAILABLE, "");
            }
            final long readerId = request.getReaderId();
            final FileReader fileReader = getFileReader(readerId);
            if (fileReader == null) {
                throw  new PacificaException(PacificaErrorCode.TIMEOUT, "");
            }
            final ByteBuffer buffer = ByteBuffer.allocate(request.getLength());
            final RpcRequest.GetFileResponse.Builder responseBuilder = RpcRequest.GetFileResponse.newBuilder();
            int readLength = fileReader.read(buffer, request.getFilename(), request.getOffset(), request.getLength());
            //set data
            if (buffer.hasRemaining()) {
                responseBuilder.setData(ByteString.copyFrom(buffer));
            } else {
                responseBuilder.setData(ByteString.empty());
            }
            // eof
            if (readLength == FileReader.EOF) {
                responseBuilder.setEof(true);
            }

        } catch (Throwable throwable) {
            ThreadUtil.runCallback(callback, Finished.failure(throwable));
        }
        return null;
    }

    protected long allocateReaderId() {
        return this.readerIdAllocator.incrementAndGet();
    }

}
