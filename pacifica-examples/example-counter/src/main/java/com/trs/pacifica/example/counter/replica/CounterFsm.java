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

package com.trs.pacifica.example.counter.replica;

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.core.fsm.BaseStateMachine;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import com.trs.pacifica.util.BitUtil;
import com.trs.pacifica.util.thread.ThreadUtil;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CounterFsm extends BaseStateMachine {

    static final String DEFAULT_SNAPSHOT_FILENAME = "_meta_replica_fsm";
    private final AtomicLong value = new AtomicLong(0);


    @Override
    public void onError(PacificaException fault) {

    }

    @Override
    public void doApply(LogId logId, ByteBuffer logData, @Nullable Callback callback) throws Exception {
        OperationClosure operationClosure = null;
        CounterOperation counterOperation = null;
        if (callback != null) {
            operationClosure = (OperationClosure) callback;
            counterOperation = operationClosure.getOperation();
        } else {
            byte[] bytes = new byte[logData.limit()];
            logData.get(bytes, 0, bytes.length);
            counterOperation = CounterOperation.fromBytes(bytes);
        }
        if (counterOperation != null) {
            replayOperation(counterOperation, operationClosure);
        }
    }

    @Override
    public void onSnapshotLoad(SnapshotReader snapshotReader) throws PacificaException {
        String snapshotDirPath = snapshotReader.getDirectory();
        File file = new File(snapshotDirPath, DEFAULT_SNAPSHOT_FILENAME);
        if (file.exists() && file.isFile()) {
            try {
                loadFromFile(file);
            } catch (Throwable e) {
                throw new PacificaException(PacificaErrorCode.IO, "failed to load snapshot.", e);
            }
        }
    }

    @Override
    public void onSnapshotSave(SnapshotWriter snapshotWriter) throws PacificaException {
        String snapshotDirPath = snapshotWriter.getDirectory();
        String filename = DEFAULT_SNAPSHOT_FILENAME;
        File file = new File(snapshotDirPath, filename);
        try {
            saveToFile(file);
            snapshotWriter.addFile(filename);
        } catch (Throwable e) {
            throw new PacificaException(PacificaErrorCode.IO, "failed to save snapshot.", e);
        }
    }

    public long incrementAndGet(long delta) {
        return this.value.addAndGet(delta);
    }

    private Object doReplayOperation(CounterOperation operation) {
        Objects.requireNonNull(operation, "operation");
        final int type = operation.getType();
        Object result = null;
        switch (type) {
            case CounterOperation.OP_TYPE_INCREMENT_AND_GET: {
                result = incrementAndGet(operation.getDelta());
                break;
            }
        }
        return result;
    }

    private void replayOperation(CounterOperation operation, OperationClosure closure) {
        try {
            Object result = doReplayOperation(operation);
            if (closure != null) {
                closure.setResult(result);
                ThreadUtil.runCallback(closure, Finished.success());
            }
        } catch (Throwable e) {
            ThreadUtil.runCallback(closure, Finished.failure(e));
        }

    }

    private void loadFromFile(File file) throws IOException {
        this.value.set(0);
        try (FileInputStream inputStream = new FileInputStream(file)) {
            byte[] valueBytes = new byte[Long.BYTES];
            inputStream.read(valueBytes);
            long value = BitUtil.getLong(valueBytes, 0);
            this.value.set(value);
        }
    }

    private void saveToFile(File file) throws IOException {
        byte[] bytes = new byte[Long.BYTES];
        BitUtil.putLong(bytes, 0, this.value.get());
        try (FileOutputStream outputStream = new FileOutputStream(file);) {
            outputStream.write(bytes);
        }
    }

}
