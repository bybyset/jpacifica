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

import com.google.protobuf.ByteString;
import com.trs.pacifica.LogManager;
import com.trs.pacifica.SnapshotManager;
import com.trs.pacifica.SnapshotStorageFactory;
import com.trs.pacifica.StateMachineCaller;
import com.trs.pacifica.async.Callback;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.snapshot.storage.DefaultSnapshotMeta;
import com.trs.pacifica.snapshot.storage.DefaultSnapshotStorage;
import com.trs.pacifica.test.BaseStorageTest;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.thread.ThreadUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class SnapshotManagerImplTest extends BaseStorageTest {


    private ReplicaImpl replica;
    private LogManager logManager;
    private StateMachineCaller stateMachineCaller;
    private PacificaClient pacificaClient;
    private SnapshotStorageFactory snapshotStorageFactory;
    private DefaultSnapshotStorage snapshotStorage;

    private SnapshotManagerImpl snapshotManager;

    private Executor downloadExecutor = Executors.newFixedThreadPool(2);

    private ReplicaOption replicaOption;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        this.replica = Mockito.mock(ReplicaImpl.class);
        Mockito.when(this.replica.getReplicaId()).thenReturn(new ReplicaId("test_group", "test_node"));

        this.logManager = Mockito.mock(LogManager.class);
        mockStateMachineCaller();
        mockPacificaClient();
        mockSnapshotStorageFactory();
        this.snapshotManager = new SnapshotManagerImpl(replica);
        this.replicaOption = new ReplicaOption();
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

    private void mockPacificaClient() {
        this.pacificaClient = Mockito.mock(PacificaClient.class);
        Mockito.doAnswer(invocation -> {
            RpcRequest.GetFileRequest request = invocation.getArgument(0, RpcRequest.GetFileRequest.class);
            RpcRequestFinished<RpcRequest.GetFileResponse> callback = invocation.getArgument(1, RpcRequestFinished.class);
            RpcRequest.GetFileResponse response = null;
            if ("_snapshot_meta".equals(request.getFilename())) {
                DefaultSnapshotMeta defaultSnapshotMeta = DefaultSnapshotMeta.newSnapshotMeta(new LogId(1003, 1));
                defaultSnapshotMeta.addFile("test1");
                defaultSnapshotMeta.addFile("test2");
                List<byte[]> bytesList = DefaultSnapshotMeta.encode(defaultSnapshotMeta);
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                for (byte[] bytes : bytesList) {
                    buffer.put(bytes);
                }
                buffer.flip();
                response = RpcRequest.GetFileResponse.newBuilder()//
                        .setEof(true)//
                        .setData(ByteString.copyFrom(buffer))//
                        .setReadLength(buffer.limit())//
                        .build();
            } else {
                byte[] data = "hello".getBytes();
                response = RpcRequest.GetFileResponse.newBuilder()//
                        .setEof(true)//
                        .setData(ByteString.copyFrom(data))//
                        .setReadLength(data.length)//
                        .build();
            }
            callback.setRpcResponse(response);
            ThreadUtil.runCallback(callback, Finished.success());
            return null;
        }).when(this.pacificaClient).getFile(Mockito.any(), Mockito.any(), Mockito.anyInt());

    }

    private void mockStateMachineCaller() {
        this.stateMachineCaller = Mockito.mock(StateMachineCaller.class);
        Mockito.doAnswer(invocation -> {
            StateMachineCaller.SnapshotLoadCallback callback = invocation.getArgument(0, StateMachineCaller.SnapshotLoadCallback.class);
            ThreadUtil.runCallback(callback, Finished.success());
            return true;
        }).when(this.stateMachineCaller).onSnapshotLoad(Mockito.any());

        Mockito.doAnswer(invocation -> {
            StateMachineCaller.SnapshotSaveCallback callback = invocation.getArgument(0, StateMachineCaller.SnapshotSaveCallback.class);
            callback.start(new LogId(1003, 1));
            ThreadUtil.runCallback(callback, Finished.success());
            return null;
        }).when(this.stateMachineCaller).onSnapshotSave(Mockito.any());

    }
    private void mockSnapshotStorageFactory() throws PacificaException, IOException {
        this.snapshotStorageFactory = Mockito.mock(SnapshotStorageFactory.class);
        this.snapshotStorage = new DefaultSnapshotStorage(this.path);
        this.snapshotStorage = Mockito.spy(snapshotStorage);
        final DefaultSnapshotStorage snapshotStorage = this.snapshotStorage;
        Mockito.doAnswer(invocation -> {
            snapshotStorage.load();
            return snapshotStorage;
        }).when(this.snapshotStorageFactory).newSnapshotStorage(Mockito.anyString());
    }

    private void mockFsmCallerOnSnapshotLoad() {
        Mockito.doAnswer(invocation -> {
            StateMachineCaller.SnapshotLoadCallback callback = invocation.getArgument(0, StateMachineCaller.SnapshotLoadCallback.class);
            callback.run(Finished.success());
            return null;
        }).when(this.stateMachineCaller).onSnapshotLoad(Mockito.any());
    }

    private void mockSnapshotDir() throws IOException {
        File snapshotDir = new File(this.path, "snapshot_" + 1003);
        snapshotDir.mkdir();
        DefaultSnapshotMeta meta = DefaultSnapshotMeta.newSnapshotMeta(new LogId(1003, 1));
        File snapshotMetaFile = new File(snapshotDir, "_snapshot_meta");
        DefaultSnapshotMeta.saveToFile(meta, snapshotMetaFile.getPath(), true);
    }

    @Test
    public void testStartupOnEmpty() throws PacificaException {
        this.snapshotManager.startup();
        Assertions.assertEquals(new LogId(0, 0), this.snapshotManager.getLastSnapshotLodId());
    }

    @Test
    public void testStartupOnNonEmpty() throws PacificaException, IOException {
        mockSnapshotDir();
        this.snapshotManager.startup();
        Assertions.assertEquals(new LogId(1003, 1), this.snapshotManager.getLastSnapshotLodId());
    }

    @Test
    public void testDoSnapshotEqualsLastAppliedLogIndex() throws PacificaException, InterruptedException {
        this.snapshotManager.startup();
        Assertions.assertEquals(new LogId(0, 0), this.snapshotManager.getLastSnapshotLodId());
        Mockito.doReturn(0L).when(this.stateMachineCaller).getLastAppliedLogIndex();
        CountDownLatch downLatch = new CountDownLatch(1);
        AtomicReference<Finished> atomicReference = new AtomicReference<>(null);
        Callback callback = new Callback() {
            @Override
            public void run(Finished finished) {
                atomicReference.set(finished);
                downLatch.countDown();
            }
        };
        this.snapshotManager.doSnapshot(callback);
        downLatch.await();
        Assertions.assertTrue(atomicReference.get().isOk());
    }

    @Test
    public void testDoSnapshotLessThanLastAppliedLogIndex() throws PacificaException, InterruptedException {
        this.snapshotManager.startup();
        Assertions.assertEquals(new LogId(0, 0), this.snapshotManager.getLastSnapshotLodId());
        Mockito.doReturn(1003L).when(this.stateMachineCaller).getLastAppliedLogIndex();
        CountDownLatch downLatch = new CountDownLatch(1);
        AtomicReference<Finished> atomicReference = new AtomicReference<>(null);
        Callback callback = new Callback() {
            @Override
            public void run(Finished finished) {
                atomicReference.set(finished);
                downLatch.countDown();
            }
        };
        this.snapshotManager.doSnapshot(callback);
        downLatch.await();
        Assertions.assertTrue(atomicReference.get().isOk());
        Assertions.assertEquals(new LogId(1003, 1), this.snapshotManager.getLastSnapshotLodId());

    }

    @Test
    public void testDoSnapshotLogIndexMargin() throws PacificaException, InterruptedException, IOException {
        this.replicaOption.setSnapshotLogIndexMargin(10);
        mockSnapshotDir();
        this.snapshotManager.startup();
        Assertions.assertEquals(new LogId(1003, 1), this.snapshotManager.getLastSnapshotLodId());
        Mockito.doReturn(1008L).when(this.stateMachineCaller).getLastAppliedLogIndex();
        CountDownLatch downLatch = new CountDownLatch(1);
        AtomicReference<Finished> atomicReference = new AtomicReference<>(null);
        Callback callback = new Callback() {
            @Override
            public void run(Finished finished) {
                atomicReference.set(finished);
                downLatch.countDown();
            }
        };
        this.snapshotManager.doSnapshot(callback);
        downLatch.await();
        Assertions.assertFalse(atomicReference.get().isOk());
        Assertions.assertEquals(new LogId(1003, 1), this.snapshotManager.getLastSnapshotLodId());
        Mockito.verify(this.logManager).onSnapshot(1003, 1);
    }




    @Test
    public void testInstallSnapshot() throws PacificaException, InterruptedException {

        this.snapshotManager.startup();
        Assertions.assertEquals(new LogId(0, 0), this.snapshotManager.getLastSnapshotLodId());

        RpcRequest.InstallSnapshotRequest request = RpcRequest.InstallSnapshotRequest.newBuilder()//
                .setReaderId(1L)//
                .setPrimaryId(RpcUtil.protoReplicaId(new ReplicaId("group1", "node1")))//
                .setTargetId(RpcUtil.protoReplicaId(new ReplicaId("group2", "node2")))//
                .setSnapshotLogIndex(1003)//
                .setSnapshotLogTerm(1)//
                .setTerm(1)//
                .setVersion(2)//
                .build();
        CountDownLatch downLatch = new CountDownLatch(1);
        AtomicReference<Finished> atomicReference = new AtomicReference<>(null);
        SnapshotManager.InstallSnapshotCallback callback = new SnapshotManager.InstallSnapshotCallback() {

            @Override
            public void run(Finished finished) {
                atomicReference.set(finished);
                downLatch.countDown();
            }
        };
        this.snapshotManager.installSnapshot(request, callback);
        downLatch.await();
        Assertions.assertTrue(atomicReference.get().isOk());
        Assertions.assertEquals(new LogId(1003, 1), this.snapshotManager.getLastSnapshotLodId());
        File snapshotDir = new File(this.path, "snapshot_1003");
        File file1 = new File(snapshotDir, "test1");
        File file2 = new File(snapshotDir, "test1");

        Assertions.assertTrue(file1.exists() && file1.isFile());
        Assertions.assertTrue(file2.exists() && file2.isFile());

    }
}
