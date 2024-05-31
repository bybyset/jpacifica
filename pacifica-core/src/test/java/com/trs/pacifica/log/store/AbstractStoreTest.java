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

import com.trs.pacifica.log.dir.BaseDirectory;
import com.trs.pacifica.log.store.file.AbstractFile;
import org.junit.jupiter.api.Assertions;
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

class AbstractStoreTest {
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
    AbstractStore abstractStore;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testEnsureOpen() {
        abstractStore.ensureOpen();
    }

    @Test
    void testLoad() {
    }

    @Test
    void testLoadExistedFiles() {
    }

    @Test
    void testLoadFile() {
    }

    @Test
    void testClose() {
    }

    @Test
    void testGetFileSequenceFromFilename() {
        long result = abstractStore.getFileSequenceFromFilename("filename");
        Assertions.assertEquals(0L, result);
    }

    @Test
    void testGetFileSequenceFromFile() {
    }

    @Test
    void testDoAllocateFile() {
    }

    @Test
    void testAllocateNextFile() {
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
        long result = abstractStore.getFirstLogIndex();
        verify(readLock).lock();
        verify(readLock).unlock();
        verify(writeLock).lock();
        verify(writeLock).unlock();
        Assertions.assertEquals(0L, result);
    }

    @Test
    void testGetFirstLogPosition() {
        int result = abstractStore.getFirstLogPosition();
        verify(readLock).lock();
        verify(readLock).unlock();
        verify(writeLock).lock();
        verify(writeLock).unlock();
        Assertions.assertEquals(0, result);
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
        when(nextFileSequence.get()).thenReturn(0L);
        when(flushedPosition.get()).thenReturn(0L);

        long result = abstractStore.getFlushedPosition();
        Assertions.assertEquals(0L, result);
    }

    @Test
    void testSetFlushedPosition() {
        abstractStore.setFlushedPosition(0L);
        verify(nextFileSequence).set(anyLong());
        verify(flushedPosition).set(anyLong());
    }

    @Test
    void testWaitForFlush() {
    }

    @Test
    void testToString() {
    }

    @Test
    void testCalculateFileStartOffset() {
    }
}
