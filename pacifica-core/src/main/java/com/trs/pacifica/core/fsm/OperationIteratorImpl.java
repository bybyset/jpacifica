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

import com.trs.pacifica.LogManager;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.error.NotFoundLogEntryException;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OperationIteratorImpl implements Iterator<LogEntry> {

    private final LogManager logManager;
    private final long startLogIndex;
    private final long endLogIndex;
    private final AtomicLong committingIndex;
    private final List<Callback> callbackList;

    private Callback curCallback = null;
    private LogEntry curLogEntry = null;

    private PacificaException error = null;


    public OperationIteratorImpl(LogManager logManager, final long endLogIndex, AtomicLong committingIndex, List<Callback> callbackList) {
        this.logManager = logManager;
        this.startLogIndex = committingIndex.get() + 1;
        this.endLogIndex = endLogIndex;
        this.committingIndex = committingIndex;
        this.callbackList = callbackList;
    }


    LogEntry logEntry() {
        return this.curLogEntry;
    }

    long logIndex() {
        return this.committingIndex.get();
    }

    Callback callback() {
        return this.curCallback;
    }

    public boolean hasError() {
        return this.error != null;
    }

    public PacificaException getError() {
        return this.error;
    }

    @Override
    public boolean hasNext() {
        return !hasError() && this.committingIndex.get() < this.endLogIndex;
    }


    @Override
    public LogEntry next() {
        this.curLogEntry = null;
        this.curCallback = null;
        final long prevLogIndex = this.committingIndex.get();
        if (prevLogIndex < this.endLogIndex) {
            final long currentLogIndex = prevLogIndex + 1;
            if (this.committingIndex.compareAndSet(prevLogIndex, currentLogIndex)) {
                try {
                    this.curLogEntry = this.logManager.getLogEntryAt(currentLogIndex);
                    if (curLogEntry == null) {
                        throw new NotFoundLogEntryException("not found LogEntry at logIndex=" + currentLogIndex);
                    }
                    this.curCallback = this.callbackList.get((int)(currentLogIndex - startLogIndex));
                } catch (Throwable e) {
                    this.committingIndex.set(prevLogIndex);
                    if (e instanceof PacificaException ) {
                        this.error = (PacificaException) e;
                    } else {
                        this.error = new PacificaException(PacificaErrorCode.INTERNAL, "failed to get LogEntry at logIndex=" + currentLogIndex, e);
                    }
                }
            }
        }
        return this.curLogEntry;
    }

}
