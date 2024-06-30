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
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.storage.DefaultSnapshotStorage;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SnapshotManagerImplTest extends BaseStorageTest {


    private ReplicaImpl replica;
    private LogManager logManager;
    private StateMachineCaller stateMachineCaller;
    private PacificaClient pacificaClient;
    private SnapshotStorageFactory snapshotStorageFactory;
    private DefaultSnapshotStorage snapshotStorage;

    private SnapshotManagerImpl snapshotManager;

    private Executor downloadExecutor = Executors.newFixedThreadPool(2);

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        this.replica = Mockito.mock(ReplicaImpl.class);
        Mockito.when(this.replica.getReplicaId()).thenReturn(new ReplicaId("test_group", "test_node"));

        this.logManager = Mockito.mock(LogManager.class);
        this.stateMachineCaller = Mockito.mock(StateMachineCaller.class);
        this.pacificaClient = Mockito.mock(PacificaClient.class);
        mockSnapshotStorageFactory();
        this.snapshotManager = new SnapshotManagerImpl(replica);
        ReplicaOption replicaOption = new ReplicaOption();
        replicaOption.setDownloadSnapshotExecutor(downloadExecutor);
        SnapshotManagerImpl.Option option = new SnapshotManagerImpl.Option();
        option.setStoragePath(this.path);
        option.setLogManager(this.logManager);
        option.setStateMachineCaller(this.stateMachineCaller);
        option.setPacificaClient(this.pacificaClient);
        option.setSnapshotStorageFactory(this.snapshotStorageFactory);
        option.setReplicaOption(replicaOption);
        this.snapshotManager.init(option);
    }

    @AfterEach
    public void shutdown() throws Exception {
        super.shutdown();
    }

    private void mockSnapshotStorageFactory() throws PacificaException, IOException {
        this.snapshotStorageFactory = Mockito.mock(SnapshotStorageFactory.class);
        this.snapshotStorage = new DefaultSnapshotStorage(this.path);
        this.snapshotStorage = Mockito.spy(snapshotStorage);
        this.snapshotStorage.load();
        Mockito.doReturn(this.snapshotStorage).when(this.snapshotStorageFactory).newSnapshotStorage(Mockito.anyString());
    }

    private void mockFsmCallerOnSnapshotLoad() {
        Mockito.doAnswer(invocation -> {
            StateMachineCaller.SnapshotLoadCallback callback = invocation.getArgument(0, StateMachineCaller.SnapshotLoadCallback.class);
            callback.run(Finished.success());
            return null;
        }).when(this.stateMachineCaller).onSnapshotLoad(Mockito.any());


    }


    @Test
    public void testStartupOnEmpty() throws PacificaException {
        this.snapshotManager.startup();
        Assertions.assertEquals(new LogId(0, 0), this.snapshotManager.getLastSnapshotLodId());
    }

    @Test
    public void testDoSnapshot() {
        Callback callback = new Callback() {
            @Override
            public void run(Finished finished) {

            }
        };
        this.snapshotManager.doSnapshot(callback);


    }

    @Test
    public void testInstallSnapshot() {

        this.snapshotManager.installSnapshot(null, null);


    }
}
