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
package com.trs.pacifica;

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.core.ReplicaState;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.Operation;
import com.trs.pacifica.model.ReplicaId;

public interface Replica {


    /**
     * get ID of the Replica
     * @return
     */
    public ReplicaId getReplicaId();

    /**
     * get state of the Replica
     * @param block true if thread safe is needed
     * @return
     */
    public ReplicaState getReplicaState(final boolean block);

    default public ReplicaState getReplicaState() {
        return getReplicaState(false);
    }


    /**
     *
     * @param block
     * @return ture if state of the replica is Primary
     */
    public boolean isPrimary(boolean block);

    default  public boolean isPrimary() {
        return isPrimary(false);
    }

    /**
     *
     * @param operation
     */
    public void apply(Operation operation);


    public void snapshot(Callback onFinish);


    public void recover(Callback onFinish);


    /**
     * get LogId at commit point
     * @return
     */
    public LogId getCommitPoint();


    /**
     * get LogId at snapshot
     * @return
     */
    public LogId getSnapshotLogId();


    public LogId getFirstLogId();


    public LogId getLastLogId();


}
