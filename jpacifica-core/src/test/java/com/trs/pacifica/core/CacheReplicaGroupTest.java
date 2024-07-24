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

import com.trs.pacifica.model.ReplicaGroup;
import com.trs.pacifica.model.ReplicaId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.Mockito.*;

public class CacheReplicaGroupTest {
    ReplicaGroup replicaGroup;
    CacheReplicaGroup cacheReplicaGroup;


    @BeforeEach
    void setUp() {
        mockReplicaGroup();
        this.cacheReplicaGroup = new CacheReplicaGroup(() -> {
            return this.replicaGroup;
        });
    }
    private void mockReplicaGroup() {
        this.replicaGroup = Mockito.mock(ReplicaGroup.class);
        ReplicaId primary = new ReplicaId("group", "primary");
        ReplicaId secondary1 = new ReplicaId("group", "secondary1");
        ReplicaId secondary2 = new ReplicaId("group", "secondary2");
        ReplicaId secondary3 = new ReplicaId("group", "secondary3");
        List<ReplicaId> secondaries = new ArrayList<>();
        secondaries.add(secondary1);
        secondaries.add(secondary2);
        secondaries.add(secondary3);
        Mockito.doReturn(primary).when(replicaGroup).getPrimary();
        Mockito.doReturn(secondaries).when(replicaGroup).listSecondary();
        Mockito.doReturn(1L).when(replicaGroup).getVersion();
        Mockito.doReturn(1L).when(replicaGroup).getPrimaryTerm();
        Mockito.doReturn("group").when(replicaGroup).getGroupName();
    }

    @Test
    void testClearCache() {
        cacheReplicaGroup.clearCache();
        Assertions.assertNull(this.cacheReplicaGroup.getInnerCache());
    }

    @Test
    void testGetCache() {
        ReplicaGroup result = cacheReplicaGroup.getCache();
        Assertions.assertEquals(replicaGroup, result);
    }

    @Test
    void testGetGroupName() {
        String result = cacheReplicaGroup.getGroupName();
        Assertions.assertEquals("group", result);
    }

    @Test
    void testGetPrimary() {
        ReplicaId result = cacheReplicaGroup.getPrimary();
        Assertions.assertEquals(new ReplicaId("group", "primary"), result);
    }

    @Test
    void testListSecondary() {
        List<ReplicaId> result = cacheReplicaGroup.listSecondary();
        Assertions.assertEquals(3, result.size());
    }

    @Test
    void testGetVersion() {
        long result = cacheReplicaGroup.getVersion();
        Assertions.assertEquals(1L, result);
    }

    @Test
    void testGetPrimaryTerm() {
        long result = cacheReplicaGroup.getPrimaryTerm();
        Assertions.assertEquals(1L, result);
    }
}