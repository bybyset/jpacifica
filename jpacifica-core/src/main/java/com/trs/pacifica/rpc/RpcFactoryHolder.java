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

package com.trs.pacifica.rpc;

import com.trs.pacifica.spi.JPacificaServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcFactoryHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcFactoryHolder.class);

    private static final RpcFactory RPC_FACTORY;

    static {
        RPC_FACTORY = JPacificaServiceLoader//
                .load(RpcFactory.class)//
                .first();
        LOGGER.info("use RpcFactory={}", RPC_FACTORY.getClass().getName());
    }

    private RpcFactoryHolder() {
    }

    public static RpcFactory getInstance() {
        return RPC_FACTORY;
    }

}
