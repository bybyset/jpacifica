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
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;

import java.util.List;

public interface LogStorage {

    /**
     * get the LogEntry at index
     *
     * @param index
     * @return null if it does not exist
     */
    LogEntry getLogEntry(final long index);


    /**
     *  get the LogId at index
     * @param index
     * @return null if it does not exist
     */
    LogId getLogIdAt(final long index);

    /**
     * get the index of the first log
     *
     * @return null if nothing
     */
    LogId getFirstLogId();

    /**
     * get the index of the last log
     *
     * @return null if nothing
     */
    LogId getLastLogId();


    /**
     * append LogEntry
     * @param logEntry
     * @return true if success
     * @throws PacificaException
     */
    boolean appendLogEntry(final LogEntry logEntry) throws PacificaException;;


    /**
     * append LogEntry in bulk.
     * It will always start appending from 0,
     * and the failed logs will break and return the number of successfully appended logEntries
     * @param logEntries
     * @return number of success
     * @throws PacificaException
     */
    int appendLogEntries(final List<LogEntry> logEntries) throws PacificaException;


    /**
     * truncate logs from storage's head, [first_log_index, first_index_kept) will be discarded.
     * @param firstIndexKept
     * @return first LogId. it is LogId(0 ,0) if nothing
     */
    LogId truncatePrefix(final long firstIndexKept);

    /**
     * truncate logs from storage's tail, (last_index_kept, last_log_index]  will be discarded.
     * @param lastIndexKept
     * @return last LogId. it is LogId(0 ,0) if nothing
     */
    LogId truncateSuffix(final long lastIndexKept);



    void open();

    void close();


}
