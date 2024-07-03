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
    private final AtomicLong applyingIndex;
    private final List<Callback> callbackList;

    private Callback curCallback = null;
    private LogEntry curLogEntry = null;

    private PacificaException error = null;


    /**
     * Operation iterator: range form applyingIndex to endLogIndex, containing the boundary values
     *
     * @param logManager
     * @param endLogIndex
     * @param applyingIndex
     * @param callbackList
     */
    public OperationIteratorImpl(LogManager logManager, final long endLogIndex, AtomicLong applyingIndex, List<Callback> callbackList) {
        this.logManager = logManager;
        this.startLogIndex = applyingIndex.get();
        this.endLogIndex = endLogIndex;
        this.applyingIndex = applyingIndex;
        this.callbackList = callbackList;
    }


    LogEntry logEntry() {
        return this.curLogEntry;
    }

    long logIndex() {
        return this.applyingIndex.get() - 1;
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
        return !hasError() && this.applyingIndex.get() <= this.endLogIndex;
    }


    @Override
    public LogEntry next() {
        final long currentLogIndex = this.applyingIndex.get();
        if (currentLogIndex <= this.endLogIndex) {
            try {
                LogEntry logEntry = this.logManager.getLogEntryAt(currentLogIndex);
                if (logEntry == null) {
                    throw new NotFoundLogEntryException("not found LogEntry at logIndex=" + currentLogIndex);
                }
                this.curLogEntry = logEntry;
                if (callbackList.isEmpty()) {
                    this.curCallback = null;
                } else {
                    this.curCallback = this.callbackList.get((int) (currentLogIndex - startLogIndex));
                }
                this.applyingIndex.incrementAndGet();
            } catch (Throwable e) {
                if (e instanceof PacificaException) {
                    this.error = (PacificaException) e;
                } else {
                    this.error = new PacificaException(PacificaErrorCode.INTERNAL, "failed to get LogEntry at logIndex=" + currentLogIndex, e);
                }
            }
        }
        return this.curLogEntry;
    }

    /**
     * roll back
     * @param tailCount num of roll back on tail
     */
    void rollback(long tailCount) {
        this.applyingIndex.addAndGet(Math.negateExact(tailCount));
    }

    void rollback() {
        rollback(1);
    }

}
