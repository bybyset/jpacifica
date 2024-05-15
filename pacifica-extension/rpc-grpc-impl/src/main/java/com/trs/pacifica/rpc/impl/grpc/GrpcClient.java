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

package com.trs.pacifica.rpc.impl.grpc;

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.rpc.client.RpcClient;
import com.trs.pacifica.rpc.node.Endpoint;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GrpcClient implements RpcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcClient.class);


    private final Map<Endpoint, ManagedChannel> channelContainer = new ConcurrentHashMap<>();
    private final Map<Endpoint, AtomicInteger> transientFailures = new ConcurrentHashMap<>();
    private final Map<String, Message> parserClasses;

    private Option option = null;

    public GrpcClient(Map<String, Message> parserClasses) {
        this.parserClasses = parserClasses;
    }

    @Override
    public boolean checkConnection(Endpoint endpoint, boolean createIfAbsent) {
        return false;
    }

    @Override
    public void invokeAsync(Endpoint endpoint, Object request, Object ctx, Callback callback, long timeoutMs) {

    }

    @Override
    public synchronized void init(Option option) throws PacificaException {
        this.option = option;
    }

    @Override
    public synchronized void startup() throws PacificaException {

    }

    @Override
    public synchronized void shutdown() throws PacificaException {

    }


    private ManagedChannel getChannel(final Endpoint endpoint, final boolean createIfAbsent) {
        if (createIfAbsent) {
            return this.channelContainer.computeIfAbsent(endpoint, this::createChannel);
        } else {
            return this.channelContainer.get(endpoint);
        }
    }

    private ManagedChannel createChannel(final Endpoint endpoint) {
        final ManagedChannel ch = ManagedChannelBuilder//
                .forAddress(endpoint.getIp(), endpoint.getPort()) //
                .usePlaintext() //
                .directExecutor() //
                .maxInboundMessageSize(this.option.getMaxInboundMessageSize()) //
                .build();
        LOGGER.info("Creating new channel to: {}.", endpoint);
        return ch;
    }
}
