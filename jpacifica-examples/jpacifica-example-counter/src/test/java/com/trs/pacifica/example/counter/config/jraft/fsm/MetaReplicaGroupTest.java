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
import org.junit.jupiter.api.Test;

public class MetaReplicaGroupTest {



    @Test
    void testFromBytes() {
        MetaReplicaGroup metaReplicaGroup = new MetaReplicaGroup("test_group", "node1");
        metaReplicaGroup.addReplica("node2");
        metaReplicaGroup.addReplica("node3");

        byte[] bytes = MetaReplicaGroup.toBytes(metaReplicaGroup);
        MetaReplicaGroup result = MetaReplicaGroup.fromBytes(bytes);
        Assertions.assertEquals(metaReplicaGroup.getGroupName(), result.getGroupName());
        Assertions.assertEquals(metaReplicaGroup.getPrimary(), result.getPrimary());
        Assertions.assertEquals(3, result.getAllReplica().size());
        Assertions.assertEquals(metaReplicaGroup.getAllReplica().size(), result.getAllReplica().size());

    }
}
