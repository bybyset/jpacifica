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
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class FsLogStorageTest extends BaseLogStorageTest {


    private FsLogStorage logStorage;

    public FsLogStorageTest() throws IOException {
        super();
    }
    @Override
    public LogStorage getLogStorage() {
        return logStorage;
    }

    @Override
    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        final String path = this.path;
        final FsLogStorageOption option = new FsLogStorageOption();
        option.setSegmentFileSize(100 * 1024);//100k
        logStorage = new FsLogStorage(path, this.logEntryEncoder, this.logEntryDecoder, option);
        logStorage.open();
    }

    @Override
    @AfterEach
    public void shutdown() throws Exception {
        logStorage.close();
        super.shutdown();
    }


    @Test
    public void testReload() throws PacificaException, IOException {
        LogEntry logEntry1 = new LogEntry(1, 1, LogEntry.Type.NO_OP);
        logStorage.appendLogEntry(logEntry1);
        LogEntry logEntry2 = new LogEntry(2, 1, LogEntry.Type.NO_OP);
        logStorage.appendLogEntry(logEntry2);
        LogEntry logEntry3 = new LogEntry(3, 1, LogEntry.Type.NO_OP);
        logStorage.appendLogEntry(logEntry3);
        LogEntry logEntry4 = new LogEntry(4, 1, LogEntry.Type.NO_OP);
        logStorage.appendLogEntry(logEntry4);
        this.logStorage.close();

        final FsLogStorageOption option = new FsLogStorageOption();
        option.setSegmentFileSize(100 * 1024);//100k
        logStorage = new FsLogStorage(path, this.logEntryEncoder, this.logEntryDecoder, option);
        logStorage.open();
        LogId firstLogId = this.logStorage.getFirstLogId();
        Assertions.assertNotNull(firstLogId);
        Assertions.assertEquals(logEntry1.getLogId(), firstLogId);

        LogId lastLogId = this.logStorage.getLastLogId();
        Assertions.assertNotNull(lastLogId);
        Assertions.assertEquals(logEntry4.getLogId(), lastLogId);
    }
}
