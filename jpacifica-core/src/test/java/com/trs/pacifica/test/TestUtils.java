/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trs.pacifica.test;

import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Test helper
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Apr-11 10:16:07 AM
 */
public class TestUtils {


    public static String mkTempDir() {
        return Paths.get(System.getProperty("java.io.tmpdir", "/tmp"), "jpacifica_test_" + System.nanoTime()).toString();
    }

    public static LogEntry mockEntry(final long index, final long term) {
        return mockEntry(index, term, 0);
    }

    public static LogEntry mockEntry(final LogId logId) {
        return mockEntry(logId.getIndex(), logId.getTerm(), 0);
    }

    public static LogEntry mockEntry(final long index, final long term, final int dataSize) {
        LogEntry entry = new LogEntry(index, term, LogEntry.Type.OP_DATA);
        if (dataSize > 0) {
            byte[] bs = new byte[dataSize];
            entry.setLogData(ByteBuffer.wrap(bs));
        }
        return entry;
    }

    public static List<LogEntry> mockEntries(final int n) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            LogEntry entry = mockEntry(i, i);
            entry.setLogData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
            entries.add(entry);
        }
        return entries;
    }
}
