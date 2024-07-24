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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public class DownloadSessionTest {


    private DownloadSession downloadSession;

    private PacificaClient pacificaClient;

    private ReplicaId targetId = new ReplicaId("test_group", "test_node");

    private long readId = 1L;

    private String fileName = "test1";



    @BeforeEach
    public void setup() {
        this.pacificaClient = Mockito.mock(PacificaClient.class);
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
        }).when(this.pacificaClient).getFile(Mockito.any(), Mockito.any(), Mockito.anyInt());
    }


    @Test
    public void testSuccess() throws UnsupportedEncodingException, ExecutionException, InterruptedException {
        mockSuccessRpc();
        final List<byte[]> response = new ArrayList<>(3);
        this.downloadSession = new DownloadSession(this.pacificaClient, this.targetId, this.readId, this.fileName ) {
            @Override
            protected void onDownload(byte[] bytes) throws IOException {
                response.add(bytes);
            }
        };
        this.downloadSession.awaitComplete();
        Assertions.assertEquals(3, response.size());
        Assertions.assertArrayEquals("hello".getBytes("utf-8"), response.get(0));
        Assertions.assertArrayEquals("jpcafica".getBytes("utf-8"), response.get(1));
        Assertions.assertArrayEquals("world".getBytes("utf-8"), response.get(2));

        Assertions.assertTrue(downloadSession.isCompleted());
        Assertions.assertFalse(downloadSession.isCancelled());
    }

    private void mockFailureRpc() throws UnsupportedEncodingException {
        Mockito.doAnswer( invocation -> {
            RpcRequestFinished<RpcRequest.GetFileResponse> callback = invocation.getArgument(1, RpcRequestFinished.class);
            callback.run(Finished.failure(new RuntimeException("test failure")));
            return null;
        }).when(this.pacificaClient).getFile(Mockito.any(), Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void testFailure() throws UnsupportedEncodingException {
        mockFailureRpc();
        final List<byte[]> response = new ArrayList<>(3);
        this.downloadSession = new DownloadSession(this.pacificaClient, this.targetId, this.readId, this.fileName ) {
            @Override
            protected void onDownload(byte[] bytes) throws IOException {
                response.add(bytes);
            }
        };
        Assertions.assertThrows(ExecutionException.class, ()->{
            this.downloadSession.awaitComplete();
        });
        Assertions.assertEquals(0, response.size());
        Assertions.assertTrue(downloadSession.isCompleted());
        Assertions.assertFalse(downloadSession.isCancelled());

    }


}
