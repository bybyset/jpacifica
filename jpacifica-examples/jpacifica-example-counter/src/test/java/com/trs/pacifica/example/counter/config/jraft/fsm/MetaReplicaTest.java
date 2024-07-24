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

package com.trs.pacifica.example.counter.config.jraft.fsm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class MetaReplicaTest {



    @BeforeEach
    public void setup() {

    }

    @Test
    public void testFromBytes() {
        MetaReplica metaReplica = new MetaReplica("node1");
        byte[] bytes = MetaReplica.toBytes(metaReplica);

        MetaReplica metaReplica1 = MetaReplica.fromBytes(bytes);
        Assertions.assertEquals(metaReplica.getNodeId(), metaReplica1.getNodeId());
        Assertions.assertEquals(metaReplica.isSecondary(), metaReplica1.isSecondary());

    }
}