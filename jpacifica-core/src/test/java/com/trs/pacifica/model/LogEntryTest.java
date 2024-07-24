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

package com.trs.pacifica.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

class LogEntryTest {

    @Test
    void testEquals() throws UnsupportedEncodingException {

        LogEntry logEntry1 = new LogEntry(1001, 2, LogEntry.Type.OP_DATA);
        LogEntry logEntry2 = new LogEntry(1001, 2, LogEntry.Type.OP_DATA);
        Assertions.assertEquals(logEntry1, logEntry2);

        ByteBuffer logData3 = ByteBuffer.wrap("test".getBytes("utf-8"));
        LogEntry logEntry3 = new LogEntry(1001, 2, LogEntry.Type.OP_DATA, logData3);
        ByteBuffer logData4 = ByteBuffer.wrap("test".getBytes("utf-8"));
        LogEntry logEntry4 = new LogEntry(1001, 2, LogEntry.Type.OP_DATA, logData4);
        Assertions.assertEquals(logEntry3, logEntry4);

    }

}
