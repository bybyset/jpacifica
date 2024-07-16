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

package com.trs.pacifica.example.counter.config;

import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;
import com.trs.pacifica.example.counter.MetaReplicaRpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class CounterGrpcHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(CounterGrpcHelper.class);

    private CounterGrpcHelper() {

    }


    public static void registerProtobufSerializer() {
        if ("com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory".equals(RpcFactoryHelper.rpcFactory().getClass()
                .getName())) {
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(MetaReplicaRpc.AddSecondaryRequest.class.getName(),
                    MetaReplicaRpc.AddSecondaryRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(
                    MetaReplicaRpc.ChangePrimaryRequest.class.getName(),
                    MetaReplicaRpc.ChangePrimaryRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(
                    MetaReplicaRpc.RemoveSecondaryRequest.class.getName(),
                    MetaReplicaRpc.RemoveSecondaryRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(
                    MetaReplicaRpc.GetReplicaGroupRequest.class.getName(),
                    MetaReplicaRpc.GetReplicaGroupRequest.getDefaultInstance());
            RpcFactoryHelper.rpcFactory().registerProtobufSerializer(
                    MetaReplicaRpc.AddReplicaRequest.class.getName(),
                    MetaReplicaRpc.AddReplicaRequest.getDefaultInstance());

            try {
                Class<?> clazz = Class.forName("com.alipay.sofa.jraft.rpc.impl.MarshallerHelper");
                Method registerRespInstance = clazz.getMethod("registerRespInstance", String.class, Message.class);
                registerRespInstance.invoke(null, MetaReplicaRpc.AddSecondaryRequest.class.getName(),
                        MetaReplicaRpc.AddSecondaryResponse.getDefaultInstance());
                registerRespInstance.invoke(null, MetaReplicaRpc.ChangePrimaryRequest.class.getName(),
                        MetaReplicaRpc.ChangePrimaryResponse.getDefaultInstance());
                registerRespInstance.invoke(null, MetaReplicaRpc.RemoveSecondaryRequest.class.getName(),
                        MetaReplicaRpc.RemoveSecondaryResponse.getDefaultInstance());
                registerRespInstance.invoke(null, MetaReplicaRpc.GetReplicaGroupRequest.class.getName(),
                        MetaReplicaRpc.GetReplicaGroupResponse.getDefaultInstance());
                registerRespInstance.invoke(null, MetaReplicaRpc.AddReplicaRequest.class.getName(),
                        MetaReplicaRpc.AddReplicaResponse.getDefaultInstance());
            } catch (Exception e) {
                LOGGER.error("Failed to init grpc server", e);
            }
        }
    }
}
