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

import java.util.Objects;

public class ReplicaId {

    private final String groupName;

    private final String nodeId;

    public ReplicaId(String groupName, String nodeId) {
        this.groupName = groupName;
        this.nodeId = nodeId;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaId replicaId = (ReplicaId) o;
        return Objects.equals(groupName, replicaId.groupName) && Objects.equals(nodeId, replicaId.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupName, nodeId);
    }

    @Override
    public String toString() {
        return this.groupName + "_" + this.nodeId;
    }

    public static ReplicaId from(final String groupName, final String nodeId) {
        return new ReplicaId(groupName, nodeId);
    }
}
