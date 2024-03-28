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

import com.trs.pacifica.util.Checksum;
import com.trs.pacifica.util.CrcUtil;

import java.nio.ByteBuffer;

public class LogEntry implements Checksum {

    static final ByteBuffer EMPTY_DATA = ByteBuffer.allocate(0);

    private final LogId logId = new LogId(0, 0);

    private ByteBuffer logData = EMPTY_DATA;

    private final Type type;

    private long checksum = 0L;

    private boolean hasChecksum = false;

    public LogEntry(Type type) {
        this.type = type;
    }

    public LogEntry(LogId logId, Type type) {
        this(logId.getIndex(), logId.getTerm(), type, EMPTY_DATA);
    }

    public LogEntry(final long logIndex, final long logTerm, final Type type, final ByteBuffer logData) {
        this.logId.setIndex(logIndex);
        this.logId.setTerm(logTerm);
        this.logData = logData;
        this.type = type;
    }

    public LogId getLogId() {
        return logId;
    }

    public ByteBuffer getLogData() {
        return logData;
    }

    public void setLogData(ByteBuffer logData) {
        this.logData = logData;
    }

    public Type getType() {
        return type;
    }

    public void setLogIndex(final long logIndex) {
        this.logId.setIndex(logIndex);
    }

    public void setLogTerm(final long logTerm) {
        this.logId.setTerm(logTerm);
    }

    public void setChecksum(long checksum) {
        this.checksum = checksum;
        this.hasChecksum = true;
    }

    public long getChecksum() {
        return checksum;
    }

    public boolean hasChecksum() {
        return hasChecksum;
    }

    public boolean isCorrupted() {
        return this.hasChecksum && this.checksum != this.checksum();
    }

    @Override
    public long checksum() {
        long c = checksum(this.type.ordinal(), this.logId.checksum());
        if (this.logData != null && this.logData.hasRemaining()) {
            c = checksum(c, CrcUtil.crc64(this.logData));
        }
        return c;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "logId=" + logId +
                ", type=" + type +
                ", checksum=" + checksum +
                ", hasChecksum=" + hasChecksum +
                '}';
    }

    public static enum Type {
        OP_DATA,
        NO_OP;
    }
}
