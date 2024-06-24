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

import com.trs.pacifica.LogManager;
import com.trs.pacifica.SnapshotStorageFactory;
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SnapshotManagerImplTest extends BaseStorageTest {


    private ReplicaImpl replica;

    private LogManager logManager;
    private StateMachineCaller stateMachineCaller;
    private PacificaClient pacificaClient;
    private SnapshotStorageFactory snapshotStorageFactory;

    private SnapshotManagerImpl snapshotManager;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        this.replica = Mockito.mock(ReplicaImpl.class);
        Mockito.when(this.replica.getReplicaId()).thenReturn(new ReplicaId("test_group", "test_node"));

        this.logManager = Mockito.mock(LogManager.class);
        this.stateMachineCaller = Mockito.mock(StateMachineCaller.class);
        this.pacificaClient = Mockito.mock(PacificaClient.class);
        this.snapshotStorageFactory = Mockito.mock(SnapshotStorageFactory.class);
        this.snapshotManager = new SnapshotManagerImpl(replica);

        SnapshotManagerImpl.Option option = new SnapshotManagerImpl.Option();
        option.setStoragePath(this.path);
        option.setLogManager(this.logManager);
        option.setStateMachineCaller(this.stateMachineCaller);
        option.setPacificaClient(this.pacificaClient);
        option.setSnapshotStorageFactory(this.snapshotStorageFactory);
        option.setReplicaOption(new ReplicaOption());


        this.snapshotManager.init(option);
        this.snapshotManager.startup();

    }

    @AfterEach
    public void shutdown() throws Exception {
        super.shutdown();
    }

}
