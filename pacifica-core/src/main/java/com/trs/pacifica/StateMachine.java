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

import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.fsm.OperationIterator;
import com.trs.pacifica.snapshot.SnapshotReader;
import com.trs.pacifica.snapshot.SnapshotWriter;

public interface StateMachine {

    /**
     * @param iterator
     */
    void onApply(OperationIterator iterator);

    /**
     * User defined snapshot load function get and load snapshot.
     * @param snapshotReader
     * @throws PacificaException
     */
    void onSnapshotLoad(final SnapshotReader snapshotReader) throws PacificaException;

    /**
     *
     * @param snapshotWriter
     * @throws PacificaException
     */
    void onSnapshotSave(final SnapshotWriter snapshotWriter) throws PacificaException;

    /**
     * Invoked once when the replica was shutdown.
     */
    void onShutdown();


    /**
     * This method is called when a critical error was encountered,
     * after this point, no any further modification is allowed to
     * apply to this replica until the error is fixed and this replica restarts.
     *
     * @param fault
     */
    void onError(PacificaException fault);


}
