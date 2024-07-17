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

package com.trs.pacifica.model;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class ReplicaGroupImpl implements ReplicaGroup{

    private final String groupName;
    private final long version;
    private final long term;
    private final ReplicaId primary;
    private final List<ReplicaId> secondary;

    public ReplicaGroupImpl(String groupName, long version, long term, ReplicaId primary, List<ReplicaId> secondary) {
        this.groupName = groupName;
        this.version = version;
        this.term = term;
        this.primary = primary;
        this.secondary = secondary;
    }

    public ReplicaGroupImpl(String groupName, long version, long term, ReplicaId primary) {
        this(groupName, version, term, primary, new ArrayList<>());
    }

    public ReplicaGroupImpl(String groupName, long version, long term, ReplicaId primary, ReplicaId... secondary) {
        this(groupName, version, term, primary, Lists.newArrayList(secondary));
    }

    public ReplicaGroupImpl(String groupName, long version, long term, String primaryNodeId) {
        this(groupName, version, term, new ReplicaId(groupName, primaryNodeId), new ArrayList<>());
    }

    public ReplicaGroupImpl(String groupName, long version, long term, String primaryNodeId, Collection<String> secondaryNodeIds) {
        List<ReplicaId> secondary = new ArrayList<>();
        for (String secondaryNodeId : secondaryNodeIds) {
            secondary.add(new ReplicaId(groupName, secondaryNodeId));
        }
        this.groupName = groupName;
        this.version = version;
        this.term = term;
        this.primary = new ReplicaId(groupName, primaryNodeId);
        this.secondary = secondary;
    }


    @Override
    public String getGroupName() {
        return this.groupName;
    }

    @Override
    public ReplicaId getPrimary() {
        return this.primary;
    }

    @Override
    public List<ReplicaId> listSecondary() {
        return this.secondary;
    }

    @Override
    public long getVersion() {
        return this.version;
    }

    @Override
    public long getPrimaryTerm() {
        return this.term;
    }
}
