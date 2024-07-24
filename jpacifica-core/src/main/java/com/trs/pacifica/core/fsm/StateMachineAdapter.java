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

package com.trs.pacifica.core.fsm;

import com.trs.pacifica.StateMachine;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StateMachineAdapter implements StateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachineAdapter.class);

    @Override
    public void onSnapshotLoad(final SnapshotReader snapshotReader) throws PacificaException {
        LOGGER.info("the StateMachine({}, {}) load snapshot.", this.getClass().getSimpleName(), this);
        return ;
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter snapshotWriter) throws PacificaException  {
        LOGGER.info("the StateMachine({}, {}) save snapshot.", this.getClass().getSimpleName(), this);
        return ;
    }

    @Override
    public void onShutdown() {
        LOGGER.info("the StateMachine({}, {}) shutdown.", this.getClass().getSimpleName(), this);
    }
}
