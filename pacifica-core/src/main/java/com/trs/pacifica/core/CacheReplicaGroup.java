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

import java.util.List;
import java.util.function.Supplier;

public class CacheReplicaGroup implements ReplicaGroup {

    private ReplicaGroup cache = null;


    private final Supplier<ReplicaGroup> cacheGetter;

    public CacheReplicaGroup(Supplier<ReplicaGroup> cacheGetter) {
        this.cacheGetter = cacheGetter;
    }

    public CacheReplicaGroup(ReplicaGroup cache, Supplier<ReplicaGroup> cacheGetter) {
        this.cache = cache;
        this.cacheGetter = cacheGetter;
    }

    public synchronized void clearCache() {
        this.cache = null;
    }


    protected ReplicaGroup getCache() {
        ReplicaGroup replicaGroup = this.cache;
        if (replicaGroup == null) {
            synchronized (this) {
                replicaGroup = this.cache;
                if (replicaGroup == null) {
                    replicaGroup = this.cache = cacheGetter.get();
                }
            }
        }
        return replicaGroup;
    }

    @Override
    public String getGroupName() {
        ReplicaGroup replicaGroup = getCache();
        if (replicaGroup != null) {
            return replicaGroup.getGroupName();
        }
        return null;
    }

    @Override
    public ReplicaId getPrimary() {
        ReplicaGroup replicaGroup = getCache();
        if (replicaGroup != null) {
            return replicaGroup.getPrimary();
        }
        return null;
    }

    @Override
    public List<ReplicaId> listSecondary() {
        ReplicaGroup replicaGroup = getCache();
        if (replicaGroup != null) {
            return replicaGroup.listSecondary();
        }
        return null;
    }

    @Override
    public long getVersion() {
        ReplicaGroup replicaGroup = getCache();
        if (replicaGroup != null) {
            return replicaGroup.getVersion();
        }
        return -1L;
    }

    @Override
    public long getPrimaryTerm() {
        ReplicaGroup replicaGroup = getCache();
        if (replicaGroup != null) {
            return replicaGroup.getPrimaryTerm();
        }
        return -1L;
    }
}
