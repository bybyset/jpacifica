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

package com.trs.pacifica.fs.remote;

import com.google.protobuf.ByteString;
import com.trs.pacifica.async.Finished;
import com.trs.pacifica.model.ReplicaId;
import com.trs.pacifica.proto.RpcRequest;
import com.trs.pacifica.rpc.RpcRequestFinished;
import com.trs.pacifica.rpc.client.PacificaClient;
import com.trs.pacifica.test.BaseStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.*;
import java.util.ArrayDeque;
import java.util.Queue;

public class RemoteFileDownloaderTest extends BaseStorageTest {


    private RemoteFileDownloader remoteFileDownloader;

    private PacificaClient pacificaClient;

    private ReplicaId remoteId = new ReplicaId("test_group", "test_node");

    private long readId = 1L;


    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        this.pacificaClient = Mockito.mock(PacificaClient.class);
        this.remoteFileDownloader = new RemoteFileDownloader(this.pacificaClient, this.remoteId, this.readId);
    }

    @AfterEach
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }

    @Test
    public void testDownloadToFile() throws IOException {
        mockSuccessRpc();
        File file = new File(this.path, "test1");
        this.remoteFileDownloader.downloadToFile("test1", file);
        Assertions.assertTrue(file.exists());
        try (FileReader fileReader = new FileReader(file);  BufferedReader bufferedReader = new BufferedReader(fileReader);){
            String value = bufferedReader.readLine();
            Assertions.assertEquals("hellojpcaficaworld", value);
        }
    }

    private void mockSuccessRpc() throws UnsupportedEncodingException {
        Queue<byte[]> bytesRes = new ArrayDeque<>(3);
        bytesRes.add("hello".getBytes("utf-8"));
        bytesRes.add("jpcafica".getBytes("utf-8"));
        bytesRes.add("world".getBytes("utf-8"));
        Mockito.doAnswer( invocation -> {
            RpcRequestFinished<RpcRequest.GetFileResponse> callback = invocation.getArgument(1, RpcRequestFinished.class);
            byte[] bytes = bytesRes.poll();
            RpcRequest.GetFileResponse response;
            if (bytes != null) {
                response = RpcRequest.GetFileResponse.newBuilder()//
                        .setData(ByteString.copyFrom(bytes))//
                        .setReadLength(bytes.length)//
                        .setEof(false)//
                        .build();
            } else {
                response = RpcRequest.GetFileResponse.newBuilder()//
                        .setReadLength(0)//
                        .setEof(true)//
                        .build();
            }
            callback.setRpcResponse(response);
            callback.run(Finished.success());
            return null;
        }).when(this.pacificaClient).getFile(Mockito.any(), Mockito.any(), Mockito.anyLong());
    }

}
