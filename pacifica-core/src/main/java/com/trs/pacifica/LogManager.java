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
import com.trs.pacifica.error.LogEntryCorruptedException;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;

import java.util.List;

public interface LogManager {


    /**
     * The logEntries is appended in bulk,
     * and callback are executed after persistent storage.
     *
     * @param logEntries
     * @param callback   finished callback
     */
    void appendLogEntries(List<LogEntry> logEntries, AppendLogEntriesCallback callback);

    /**
     * append LogEntry
     *
     * @param logEntry
     * @param callback
     */
    default void appendLogEntry(final LogEntry logEntry, final AppendLogEntriesCallback callback) {
        appendLogEntries(List.of(logEntry), callback);
    }

    /**
     * get LogEntry at log index
     *
     * @param logIndex
     * @return
     * @throws LogEntryCorruptedException if the LogEntry corrupted
     */
    LogEntry getLogEntryAt(final long logIndex) throws LogEntryCorruptedException;

    /**
     * get term of LogId at log index
     * @param logIndex
     * @return term of LogId at logIndex.
     * @return 0 if logIndex <= 0 or not found LogEntry.
     */
    long getLogTermAt(final long logIndex);

    /**
     * get first log id
     *
     * @return LogId(0, 0) if nothing
     */
    LogId getFirstLogId();

    /**
     * get last log id
     *
     * @return LogId(0, 0) if nothing
     */
    LogId getLastLogId();


    /**
     * called when snapshot be saved or load
     *
     * @param snapshotLogIndex log index at snapshot
     * @param snapshotLogTerm  log term at snapshot
     */
    void onSnapshot(final long snapshotLogIndex, final long snapshotLogTerm);


    abstract class AppendLogEntriesCallback implements Callback {

        private long firstLogIndex;

        private int appendCount;

        public long getFirstLogIndex() {
            return firstLogIndex;
        }

        public void setFirstLogIndex(long firstLogIndex) {
            this.firstLogIndex = firstLogIndex;
        }

        public int getAppendCount() {
            return appendCount;
        }

        public void setAppendCount(int appendCount) {
            this.appendCount = appendCount;
        }
    }

}

