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

package com.trs.pacifica.fsm;

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.model.LogEntry;

import java.nio.ByteBuffer;

public class OperationIteratorWrapper implements OperationIterator {


    private final OperationIteratorImpl wrapper;

    public OperationIteratorWrapper(OperationIteratorImpl wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public ByteBuffer getLogData() {
        final LogEntry logEntry = wrapper.next();
        if (logEntry != null) {
            return logEntry.getLogData();
        }
        return null;
    }

    @Override
    public long getLogIndex() {
        final LogEntry logEntry = wrapper.next();
        if (logEntry != null) {
            return logEntry.getLogId().getIndex();
        }
        return 0L;
    }

    @Override
    public long getLogTerm() {
        final LogEntry logEntry = wrapper.next();
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
    public boolean hasNext() {
        return wrapper.hasNext() && LogEntry.Type.OP_DATA == wrapper.logEntry().getType();
    }

    @Override
    public ByteBuffer next() {
        final LogEntry logEntry = wrapper.next();
        if (logEntry != null) {
            return logEntry.getLogData();
        }
        return null;
    }
}
