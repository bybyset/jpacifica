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

package com.trs.pacifica.fs.remote;

import com.google.protobuf.ByteString;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.async.Task;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.ExecutorResponseCallback;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.util.RpcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class DownloadSession implements Task {
    static final Logger LOGGER = LoggerFactory.getLogger(DownloadSession.class);
    static final int DEFAULT_GET_FILE_REQUEST_TIMEOUT_MS = 5 * 60 * 1000;
    static final int DEFAULT_READ_LENGTH = 1024;

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile Throwable exception = null;

    private AtomicBoolean finished = new AtomicBoolean(false);

    private final PacificaClient pacificaClient;
    private final RpcRequest.GetFileRequest.Builder requestBuilder;
    private final int requestTimeoutMs = DEFAULT_GET_FILE_REQUEST_TIMEOUT_MS;
    private final int readLength = DEFAULT_READ_LENGTH;


    DownloadSession(PacificaClient pacificaClient, final ReplicaId targetId, final long readerId, final String filename) {
        this.pacificaClient = pacificaClient;
        this.requestBuilder = RpcRequest.GetFileRequest.newBuilder()
                .setTargetId(RpcUtil.protoReplicaId(targetId))//
                .setReaderId(readerId)//
                .setFilename(filename)//
                .setOffset(0)//
                .setLength(0);
        continueDownload();
    }

    void continueDownload() {
        // check finished
        if (this.finished.get()) {
            LOGGER.warn("continue download file(filename={}, read_id={}), but it is finished.", this.requestBuilder.getFilename(), this.requestBuilder.getReaderId());
            return;
        }
        final int offset = requestBuilder.getOffset() + requestBuilder.getLength();
        RpcRequest.GetFileRequest request = requestBuilder
                .setOffset(offset)//
                .setLength(readLength)//
                .build();
        pacificaClient.getFile(request, new ExecutorResponseCallback<RpcRequest.GetFileResponse>() {
            @Override
            protected void doRun(Finished finished) {
                if (finished.isOk()) {
                    handleGetFileResponse(getRpcResponse());
                } else {
                    handleGetFileError(finished.error());
                }
            }
        }, requestTimeoutMs);
    }

    void handleGetFileResponse(RpcRequest.GetFileResponse response) {
        //handle success
        try {
            ByteString byteString = response.getData();
            onDownload(byteString.toByteArray());
            if (!response.getEof()) {
                continueDownload();
            } else {
                onFinished(null);
            }
        } catch (Throwable throwable) {
            onFinished(throwable);
        }
    }

    void handleGetFileError(Throwable throwable) {
        if (throwable != null) {
            onFinished(throwable);
        }
    }

    @Override
    public void awaitComplete() throws InterruptedException, ExecutionException {
        this.latch.await();
        if (this.exception != null) {
            throw new ExecutionException(exception);
        }
    }


    private boolean onFinished(final Throwable throwable) {
        if (finished.compareAndSet(false, true)) {
            this.exception = throwable;
            this.latch.countDown();
            return true;
        }
        return false;
    }

    /**
     * Attempts to cancel execution of this task.
     * This method has no effect if the task is already completed or cancelled,
     * or could not be cancelled for some other reason.
     * @return false if the task could not be cancelled, typically because it has already completed;
     * true otherwise.  If two or more threads cause a task to be cancelled, then at least one of them returns true.
     */
    @Override
    public boolean cancel() {
       return onFinished(new CancellationException("cancel download task."));
    }

    protected abstract void onDownload(byte[] bytes) throws IOException;

    @Override
    public boolean isCancelled() {
        return this.exception != null && this.exception instanceof CancellationException;
    }

    @Override
    public boolean isCompleted() {
        return finished.get();
    }
}
