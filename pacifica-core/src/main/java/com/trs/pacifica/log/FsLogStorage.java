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

package com.trs.pacifica.log;

import com.trs.pacifica.LogStorage;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.model.LogId;

import java.util.List;
import java.util.Objects;

/**
 * base on FileSystem implement
 */
public class FsLogStorage implements LogStorage {

    private final String storagePath;

    public FsLogStorage(String storagePath) {
        this.storagePath = Objects.requireNonNull(storagePath, "storagePath");
    }



    @Override
    public LogEntry getLogEntry(long index) {
        return null;
    }

    @Override
    public LogId getLogIdAt(int index) {
        return null;
    }

    @Override
    public LogId getFirstLogIndex() {
        return null;
    }

    @Override
    public LogId getLastLogIndex() {
        return null;
    }
    @Override
    public boolean appendLogEntry(LogEntry logEntry) {
        return false;
    }

    @Override
    public int appendLogEntries(List<LogEntry> logEntries) {
        return 0;
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        return false;
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        return false;
    }

    @Override
    public void close() {

    }
}
