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

import com.trs.pacifica.async.Callback;
import com.trs.pacifica.model.LogId;
import com.trs.pacifica.model.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public abstract class BaseStateMachine extends StateMachineAdapter {

    static final Logger LOGGER = LoggerFactory.getLogger(BaseStateMachine.class);

    @Override
    public void onApply(OperationIterator iterator) {
        try {
            while (iterator.hasNext()) {
                iterator.next();
                final ByteBuffer logData = iterator.getLogData();
                final long logIndex = iterator.getLogIndex();
                final long logTerm = iterator.getLogTerm();
                final Callback callback = iterator.getCallback();
                //do something
                doApply(new LogId(logIndex, logTerm), logData, callback);

            }
        } catch (Throwable throwable) {
            LOGGER.error("Failed to onApply, we will break and rollback it.", throwable);
            iterator.breakAndRollback(throwable);
        }
    }


    /**
     * Apply the op log to the state machine
     *
     * @param logId
     * @param callback specified by user, to see {@link Operation#getOnFinish()}, Non-Primary it is null
     * @param logData  specified by user, to see {@link Operation#getLogData()},
     * @throws Exception
     */
    public abstract void doApply(final LogId logId, final ByteBuffer logData, @Nullable final Callback callback) throws Exception;


}
