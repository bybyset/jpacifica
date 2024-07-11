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

package com.trs.pacifica.example.counter.replica;

import com.trs.pacifica.util.BitUtil;
import org.apache.commons.lang.ArrayUtils;

import java.util.Objects;

public class CounterOperation {

    public static final int OP_TYPE_INCREMENT_AND_GET = 1;


    private final int type;

    private long delta;


    public CounterOperation(int type) {
        this.type = type;
    }

    public CounterOperation(int type, long delta) {
        this.type = type;
        this.delta = delta;
    }

    public int getType() {
        return type;
    }

    public long getDelta() {
        return delta;
    }

    public void setDelta(long delta) {
        this.delta = delta;
    }


    public static CounterOperation createIncrement(final long delta) {
        return new CounterOperation(OP_TYPE_INCREMENT_AND_GET, delta);
    }


    public static final byte[] toBytes(CounterOperation operation) {
        Objects.requireNonNull(operation, "operation");
        byte[] typeBytes = new byte[Integer.BYTES];
        BitUtil.putInt(typeBytes, 0, operation.type);
        byte[] deltaBytes = new byte[Long.BYTES];
        BitUtil.putLong(deltaBytes, 0, operation.delta);
        byte[] bytes = ArrayUtils.addAll(typeBytes, deltaBytes);
        return bytes;
    }

    public static final CounterOperation fromBytes(byte[] bytes) {
        int offset = 0;
        final int type = BitUtil.getInt(bytes, offset);
        offset += Integer.BYTES;
        final long delta = BitUtil.getLong(bytes, offset);
        offset += Long.BYTES;
        return new CounterOperation(type, delta);
    }


}
