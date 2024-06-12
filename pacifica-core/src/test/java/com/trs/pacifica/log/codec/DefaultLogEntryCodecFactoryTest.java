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

package com.trs.pacifica.log.codec;

import com.google.protobuf.ByteString;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.util.io.DataBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class DefaultLogEntryCodecFactoryTest {
    DefaultLogEntryCodecFactory defaultLogEntryCodecFactory = new DefaultLogEntryCodecFactory();

    LogEntryDecoder logEntryDecoder = defaultLogEntryCodecFactory.getLogEntryDecoder();

    LogEntryEncoder logEntryEncoder = defaultLogEntryCodecFactory.getLogEntryEncoder();

    @Test
    public void testEncode() {
        final long logIndex = 1001;
        final long logTerm = 2;
        final ByteBuffer logData = ByteBuffer.wrap("test".getBytes());
        ByteBuffer testLogData = logData.asReadOnlyBuffer();
        LogEntry logEntry = new LogEntry(logIndex, logTerm, LogEntry.Type.OP_DATA);
        logEntry.setLogData(logData);
        final DataBuffer encodes = logEntryEncoder.encode(logEntry);
        logEntry.setLogData(testLogData);
        Assertions.assertEquals(0, encodes.position());
        LogEntry logEntryDecode = logEntryDecoder.decode(encodes);
        Assertions.assertEquals(true, logEntry.equals(logEntryDecode));
    }

    @Test
    void testEncodeHeader() {
        byte[] result = DefaultLogEntryCodecFactory.encodeHeader();
        Assertions.assertArrayEquals(new byte[]{(byte) 0}, result);
    }
}
