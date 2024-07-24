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
import com.trs.pacifica.PendingQueue;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.*;

class OperationIteratorImplTest {
    LogManager logManager;
    AtomicLong applyingIndex = new AtomicLong(1);
    PendingQueue<Callback> callbackList;

    OperationIteratorImpl operationIteratorImpl;

    @BeforeEach
    void setUp() {
        this.logManager = Mockito.mock(LogManager.class);
        this.callbackList = Mockito.mock(PendingQueue.class);
        Mockito.doReturn(null).when(this.callbackList).poll(Mockito.anyInt());
        for (long logIndex = 1; logIndex <= 11; logIndex++) {
            final LogEntry log = new LogEntry(logIndex, 1, LogEntry.Type.OP_DATA);
            Mockito.when(this.logManager.getLogEntryAt(logIndex)).thenReturn(log);
        }
        operationIteratorImpl = new OperationIteratorImpl(this.logManager, 11, applyingIndex, callbackList);
    }

    @Test
    void testNext() {
        long logIndex = 1;
        while (this.operationIteratorImpl.hasNext()) {
            operationIteratorImpl.next();
            LogEntry logEntry = operationIteratorImpl.logEntry();
            Assertions.assertNotNull(logEntry);
            Assertions.assertEquals(logIndex, logEntry.getLogId().getIndex());
            Assertions.assertEquals(1, logEntry.getLogId().getTerm());
            Assertions.assertEquals(logIndex, operationIteratorImpl.logIndex());
            logIndex++;
        }
        Assertions.assertEquals(logIndex, 12);
        Assertions.assertEquals(logIndex, applyingIndex.get());
    }


    @Test
    public void testNotFoundLogEntry() {
        Mockito.when(this.logManager.getLogEntryAt(5)).thenReturn(null);
        long logIndex = 1;
        while (this.operationIteratorImpl.hasNext()) {
            operationIteratorImpl.next();
            LogEntry logEntry = operationIteratorImpl.logEntry();
            Assertions.assertNotNull(logEntry);
            if (logIndex != 5) {
                Assertions.assertEquals(logIndex, logEntry.getLogId().getIndex());
                Assertions.assertEquals(1, logEntry.getLogId().getTerm());
                Assertions.assertEquals(logIndex, operationIteratorImpl.logIndex());
            } else {
                Assertions.assertEquals(logIndex - 1, operationIteratorImpl.logIndex());
                Assertions.assertEquals(logIndex - 1, logEntry.getLogId().getIndex());
                Assertions.assertEquals(1, logEntry.getLogId().getTerm());
            }
            logIndex++;
        }
        Assertions.assertTrue(operationIteratorImpl.hasError());
        Assertions.assertEquals(5, applyingIndex.get());
    }


    @Test
    void testCallback() {
        long logIndex = 1;
        while (this.operationIteratorImpl.hasNext()) {
            operationIteratorImpl.next();
            logIndex++;
        }
        Assertions.assertEquals(logIndex, 12);
        Assertions.assertEquals(logIndex, applyingIndex.get());
        operationIteratorImpl.rollback();
        Assertions.assertEquals(logIndex - 1, applyingIndex.get());

        Assertions.assertTrue(this.operationIteratorImpl.hasNext());
        Assertions.assertNotNull(operationIteratorImpl.next());
    }

}