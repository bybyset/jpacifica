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

package com.trs.pacifica.core;

import com.trs.pacifica.LogStorageFactory;
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.thread.SingleThreadExecutor;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.log.codec.DefaultLogEntryCodecFactory;
import com.trs.pacifica.log.codec.LogEntryCodecFactory;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.test.BaseStorageTest;
import com.trs.pacifica.test.MockSingleThreadExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Objects;

public class LogManagerImplTest extends BaseStorageTest {

    private LogManagerImpl logManager;
    private ReplicaImpl replica;
    private StateMachineCaller stateMachineCaller;
    private LogStorageFactory logStorageFactory;

    private LogEntryCodecFactory logEntryCodecFactory = new DefaultLogEntryCodecFactory();
    private SingleThreadExecutor singleThreadExecutor = new MockSingleThreadExecutor();

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        this.replica = Mockito.mock(ReplicaImpl.class);
        Mockito.when(this.replica.getReplicaId()).thenReturn(new ReplicaId("test_group", "test_node"));
        this.stateMachineCaller = Mockito.mock(StateMachineCaller.class);
        this.logStorageFactory = Mockito.mock(LogStorageFactory.class);
        this.logManager = new LogManagerImpl(this.replica);

        LogManagerImpl.Option option = new LogManagerImpl.Option();
        option.setLogStoragePath(this.path);
        option.setLogManagerExecutor(singleThreadExecutor);
        option.setStateMachineCaller(this.stateMachineCaller);
        option.setLogStorageFactory(this.logStorageFactory);
        option.setLogEntryCodecFactory(this.logEntryCodecFactory);
        option.setReplicaOption(new ReplicaOption());

        this.logManager.init(option);
        this.logManager.startup();
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }


    @Test
    public void testEmptyState() {
        final LogId firstLogId = new LogId(0, 0);
        Assertions.assertEquals(firstLogId, this.logManager.getFirstLogId());
        final LogId lastLogId = new LogId(0, 0);
        Assertions.assertEquals(lastLogId, this.logManager.getLastLogId());


    }


}