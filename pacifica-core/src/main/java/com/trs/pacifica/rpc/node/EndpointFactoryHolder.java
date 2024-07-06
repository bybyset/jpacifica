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

package com.trs.pacifica.rpc.node;

import com.trs.pacifica.spi.JPacificaServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointFactoryHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(EndpointFactoryHolder.class);

    private static final EndpointFactory ENDPOINT_FACTORY;

    static {
        ENDPOINT_FACTORY = JPacificaServiceLoader//
                .load(EndpointFactory.class)//
                .first();
        LOGGER.info("use EndpointFactory={}", ENDPOINT_FACTORY.getClass().getName());
    }


    private EndpointFactoryHolder() {
    }


    public static final EndpointFactory getInstance() {
        return ENDPOINT_FACTORY;
    }
}
