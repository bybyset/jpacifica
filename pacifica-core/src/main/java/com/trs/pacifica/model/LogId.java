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

import com.trs.pacifica.util.Bits;
import com.trs.pacifica.util.Checksum;
import com.trs.pacifica.util.Copyable;
import com.trs.pacifica.util.CrcUtil;

import java.util.Objects;

public class LogId implements Checksum, Comparable<LogId>, Copyable<LogId> {

    /**
     * sequence number of, It starts at 1 and increases.
     */
    private long index = 0;

    /**
     * term number of primary, It starts at 1 and increases.
     */
    private long term = 0;

    public LogId(long index, long term) {
        this.index = index;
        this.term = term;
    }

    public LogId() {
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogId logId = (LogId) o;
        return index == logId.index && term == logId.term;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, term);
    }


    @Override
    public long checksum() {
        byte[] bs = new byte[16];
        Bits.putLong(bs, 0, this.index);
        Bits.putLong(bs, 8, this.term);
        return CrcUtil.crc64(bs);
    }

    @Override
    public int compareTo(LogId o) {
        // Compare term at first
        final int c = Long.compare(getTerm(), o.getTerm());
        if (c == 0) {
            return Long.compare(getIndex(), o.getIndex());
        } else {
            return c;
        }
    }

    @Override
    public String toString() {
        return "LogId [index=" + this.index + ", term=" + this.term + "]";
    }

    @Override
    public LogId copy() {
        return new LogId(this.index, this.term);
    }
}
