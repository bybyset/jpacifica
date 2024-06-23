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

package com.trs.pacifica.core.fsm;

import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;

import java.nio.ByteBuffer;
import java.util.Objects;

public class OperationIteratorWrapper implements OperationIterator {

    private final OperationIteratorImpl wrapper;
    private final StateMachineCaller stateMachineCaller;
    private LogEntry currentLogEntry = null;
    private Callback currentCallback = null;
    private LogEntry nextLogEntry = null;

    private volatile Throwable error;



    public OperationIteratorWrapper(OperationIteratorImpl wrapper, StateMachineCaller stateMachineCaller) {
        this.wrapper = wrapper;
        this.nextLogEntry = wrapper.logEntry();
        this.stateMachineCaller = stateMachineCaller;
    }

    @Override
    public ByteBuffer getLogData() {
        final LogEntry logEntry = this.currentLogEntry;
        if (logEntry != null) {
            return logEntry.getLogData();
        }
        return null;
    }

    @Override
    public long getLogIndex() {
        final LogEntry logEntry = this.currentLogEntry;
        if (logEntry != null) {
            return logEntry.getLogId().getIndex();
        }
        return 0L;
    }

    @Override
    public long getLogTerm() {
        final LogEntry logEntry = this.currentLogEntry;
        if (logEntry != null) {
            return logEntry.getLogId().getTerm();
        }
        return 0L;
    }

    @Override
    public Callback getCallback() {
        return wrapper.callback();
    }

    @Override
    public void breakAndRollback(Throwable throwable) {
        Objects.requireNonNull(throwable);
        this.error = throwable;
        this.wrapper.rollback();
        PacificaException pacificaException = new PacificaException(PacificaErrorCode.USER_ERROR,
                "An exception occurred by user, msg=" + throwable.getMessage(), throwable);
        this.stateMachineCaller.onError(pacificaException);
    }

    @Override
    public boolean hasNext() {
        return !hasError() && this.nextLogEntry != null && LogEntry.Type.OP_DATA == nextLogEntry.getType();
    }

    @Override
    public ByteBuffer next() {
        this.currentLogEntry = this.nextLogEntry;
        this.currentCallback = this.wrapper.callback();
        this.nextLogEntry = null;
        if (this.wrapper.hasNext()) {
            this.nextLogEntry = this.wrapper.next();
        }
        return this.currentLogEntry.getLogData();
    }

    public LogEntry getNextLogEntry() {
        return this.nextLogEntry;
    }

    public boolean hasError() {
        return this.error != null;
    }

    public Throwable getError() {
        return this.error;
    }
}
