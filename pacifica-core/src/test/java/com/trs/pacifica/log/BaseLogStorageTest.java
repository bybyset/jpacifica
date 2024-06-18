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

package com.trs.pacifica.log;

import com.trs.pacifica.LogStorage;
import com.trs.pacifica.log.codec.DefaultLogEntryCodecFactory;
import com.trs.pacifica.log.codec.LogEntryCodecFactory;
import com.trs.pacifica.log.codec.LogEntryDecoder;
import com.trs.pacifica.log.codec.LogEntryEncoder;
import com.trs.pacifica.test.BaseStorageTest;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseLogStorageTest extends BaseStorageTest {

    LogEntryEncoder logEntryEncoder = null;
    LogEntryDecoder logEntryDecoder = null;

    BaseLogStorageTest() {
        LogEntryCodecFactory logEntryCodecFactory = new DefaultLogEntryCodecFactory();
        this.logEntryEncoder = logEntryCodecFactory.getLogEntryEncoder();
        this.logEntryDecoder = logEntryCodecFactory.getLogEntryDecoder();
    }

    @Override
    public void setup() throws Exception {
        super.setup();
    }

    public abstract LogStorage getLogStorage();

    @Test
    public void testAddOneEntryState() {
        final LogStorage logStorage = getLogStorage();
        final LogId logId1 = new LogId(100, 1);
        final LogEntry entry1 = TestUtils.mockEntry(logId1);
        Assertions.assertTrue(logStorage.appendLogEntry(entry1));
        Assertions.assertEquals(logId1, logStorage.getFirstLogId());
        Assertions.assertEquals(logId1, logStorage.getLastLogId());

        final LogId logId2 = new LogId(101, 1);
        final LogEntry entry2 = TestUtils.mockEntry(logId2);
        Assertions.assertTrue(logStorage.appendLogEntry(entry2));
        Assertions.assertEquals(logId1, logStorage.getFirstLogId());
        Assertions.assertEquals(logId2, logStorage.getLastLogId());

        Assertions.assertEquals(entry1, logStorage.getLogEntry(logId1.getIndex()));
        Assertions.assertEquals(entry2, logStorage.getLogEntry(logId2.getIndex()));
    }

    @Test
    public void testAddManyEntries() {
        final LogStorage logStorage = getLogStorage();
        final List<LogEntry> entries = TestUtils.mockEntries(20);
        List<LogEntry> expecteds = toExpected(entries);
        int logEntryCount = logStorage.appendLogEntries(entries);
        Assertions.assertEquals(20, logEntryCount);
        LogId firstLogId = logStorage.getFirstLogId();
        Assertions.assertNotNull(firstLogId);
        Assertions.assertEquals(1, firstLogId.getIndex());
        LogId lastLogId = logStorage.getLastLogId();
        Assertions.assertNotNull(lastLogId);
        Assertions.assertEquals(20, lastLogId.getIndex());
        for (int i = 1; i < 21; i++) {
            final LogEntry entry = logStorage.getLogEntry(i);
            Assertions.assertNotNull(entry);
            Assertions.assertEquals(i, entry.getLogId().getTerm());
            Assertions.assertEquals(expecteds.get(i - 1), entry);
        }
    }

    @Test
    public void testReset() {
        final LogStorage logStorage = getLogStorage();
        testAddManyEntries();
        logStorage.reset(5);
        LogId firstLogId = logStorage.getFirstLogId();
        Assertions.assertNotNull(firstLogId);
        Assertions.assertEquals(5, firstLogId.getIndex());

        LogId lastLogId = logStorage.getLastLogId();
        Assertions.assertNotNull(lastLogId);
        Assertions.assertEquals(5, lastLogId.getIndex());
    }


    @Test
    public void testTruncatePrefix() {
        final LogStorage logStorage = getLogStorage();
        final List<LogEntry> entries = TestUtils.mockEntries(10);

        List<LogEntry> expecteds = toExpected(entries);
        Assertions.assertEquals(10, logStorage.appendLogEntries(entries));
        LogId firstLogIdAfter = logStorage.truncatePrefix(5);
        Assertions.assertNotNull(firstLogIdAfter);
        LogId firstLogId = logStorage.getFirstLogId();
        Assertions.assertNotNull(firstLogId);
        Assertions.assertEquals(firstLogIdAfter, firstLogId);
        Assertions.assertTrue(firstLogId.getIndex() <= 5);
        LogId lastLogId = logStorage.getLastLogId();
        Assertions.assertNotNull(lastLogId);
        Assertions.assertEquals(10, lastLogId.getIndex());
        for (int i = 1; i <= 10; i++) {
            if (i < firstLogIdAfter.getIndex()) {
                Assertions.assertNull(logStorage.getLogEntry(i));
            } else {
                Assertions.assertEquals(expecteds.get(i - 1), logStorage.getLogEntry(i));
            }
        }
    }

    @Test
    public void testTruncateSuffix() {
        final LogStorage logStorage = getLogStorage();
        final List<LogEntry> entries = TestUtils.mockEntries(7);
        List<LogEntry> expecteds = toExpected(entries);
        Assertions.assertEquals(7, logStorage.appendLogEntries(entries));
        LogId lastLogIdAfter = logStorage.truncateSuffix(5);
        LogId firtLogId = logStorage.getFirstLogId();
        Assertions.assertNotNull(firtLogId);
        Assertions.assertEquals(1, firtLogId.getIndex());

        LogId lastLogId = logStorage.getLastLogId();
        Assertions.assertNotNull(lastLogId);
        Assertions.assertEquals(lastLogIdAfter, lastLogId);
        Assertions.assertEquals(5, lastLogId.getIndex());

        for (int i = 1; i <= 7; i++) {
            if (i <= lastLogIdAfter.getIndex()) {
                Assertions.assertEquals(expecteds.get(i - 1), logStorage.getLogEntry(i));
            } else {
                Assertions.assertNull(logStorage.getLogEntry(i));
            }
        }
    }

    @Test
    public void testGetLogIdAt() {
        final LogStorage logStorage = getLogStorage();
        final List<LogEntry> entries = TestUtils.mockEntries(7);
        Assertions.assertEquals(7, logStorage.appendLogEntries(entries));

        for (int i = 1; i <= 7; i++) {
            final LogId logId = new LogId(i, i);
            Assertions.assertEquals(logId, logStorage.getLogIdAt(i));
        }
    }


    static List<LogEntry> toExpected(List<LogEntry> logEntries) {
        List<LogEntry> expecteds = new ArrayList<>(logEntries.size());
        logEntries.forEach( logEntry -> {
            LogEntry expected = new LogEntry(logEntry.getLogId(), logEntry.getType());
            expected.setLogData(logEntry.getLogData().duplicate());
            if (logEntry.hasChecksum()) {
                expected.setChecksum(logEntry.getChecksum());
            }
            expecteds.add(expected);
        });
        return expecteds;
    }
}
