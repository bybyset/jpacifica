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

package com.trs.pacifica.fsm;

import com.trs.pacifica.StateMachine;
import com.trs.pacifica.async.Callback;

import java.nio.ByteBuffer;

public abstract class AbstractStateMachine implements StateMachine {

    @Override
    public void onApply(OperationIterator iterator) {

        while (iterator.hasNext()) {

            final ByteBuffer logData = iterator.getLogData();
            final long logIndex = iterator.getLogIndex();
            final long logTerm = iterator.getLogTerm();
            final Callback callback = iterator.getCallback();


            //do something

            iterator.next();
        }


    }
}
