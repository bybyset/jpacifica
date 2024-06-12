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

package com.trs.pacifica.log.store;

import com.trs.pacifica.log.codec.DefaultLogEntryCodecFactory;
import com.trs.pacifica.log.dir.BaseDirectory;
import com.trs.pacifica.log.store.file.AbstractFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.mockito.Mockito.*;

class SegmentStoreTest {
    @Mock
    AtomicLong nextFileSequence;
    @Mock
    Deque<AbstractFile> files;
    @Mock
    ReadWriteLock lock;
    @Mock
    Lock readLock;
    @Mock
    Lock writeLock;
    @Mock
    BaseDirectory directory;
    @Mock
    AtomicLong flushedPosition;
    @InjectMocks
    SegmentStore segmentStore;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testAppendLogData() {
    }

    @Test
    void testLookupLogEntry() {
    }

    @Test
    void testNewAbstractFile() {
    }

    @Test
    void testSliceLogEntry() {
    }

    @Test
    void testSliceLogEntry2() {
    }

    @Test
    void testGetAppendLogDataByteSize() {
    }

    @Test
    void testEnsureOpen() {
        segmentStore.ensureOpen();
    }

    @Test
    void testLoad() {
    }

    @Test
    void testClose() {
    }

    @Test
    void testGetNextFile() {
    }

    @Test
    void testGetLastFile() {
    }

    @Test
    void testLookupFile() {
    }

    @Test
    void testSliceFile() {
    }

    @Test
    void testGetFirstLogIndex() {
    }

    @Test
    void testGetFirstLogPosition() {
    }

    @Test
    void testGetLastLogIndex() {
    }

    @Test
    void testTruncatePrefix() {
    }

    @Test
    void testTruncateSuffix() {
    }

    @Test
    void testTruncateSuffix2() {
    }

    @Test
    void testDeleteFile() {
    }

    @Test
    void testFlush() {
    }

    @Test
    void testGetFlushedPosition() {
    }

    @Test
    void testWaitForFlush() {
    }

}
