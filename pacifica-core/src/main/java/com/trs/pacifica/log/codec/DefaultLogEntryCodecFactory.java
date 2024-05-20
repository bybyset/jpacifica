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

package com.trs.pacifica.log.codec;

import com.google.protobuf.ByteString;
import com.trs.pacifica.model.LogEntry;
import com.trs.pacifica.proto.RpcCommon;
import com.trs.pacifica.spi.SPI;
import com.trs.pacifica.util.RpcUtil;
import com.trs.pacifica.util.io.DataBuffer;

import java.nio.ByteBuffer;

@SPI
public class DefaultLogEntryCodecFactory implements LogEntryCodecFactory{

    static final LogEntryDecoder PROTOBUF_DECODER = new ProtobufLogEntryDecoder();

    static final LogEntryEncoder PROTOBUF_ENCODER = new ProtobufLogEntryEncoder();

    @Override
    public LogEntryDecoder getLogEntryDecoder() {
        return PROTOBUF_DECODER;
    }

    @Override
    public LogEntryEncoder getLogEntryEncoder() {
        return PROTOBUF_ENCODER;
    }


    static class ProtobufLogEntryDecoder implements LogEntryDecoder {

        @Override
        public LogEntry decode(DataBuffer data) {
            return null;
        }
    }

    static class ProtobufLogEntryEncoder implements LogEntryEncoder {

        @Override
        public DataBuffer encode(LogEntry logEntry) {
            //header

            //
            RpcCommon.LogEntryPO.Builder logEntryPO = RpcCommon.LogEntryPO.newBuilder();
            logEntryPO.setType(RpcUtil.protoLogEntryType(logEntry.getType()));
            logEntryPO.setLogIndex(logEntry.getLogId().getIndex());
            logEntryPO.setLogTerm(logEntry.getLogId().getTerm());
            final ByteBuffer data = logEntry.getLogData();
            logEntryPO.setData(ByteString.copyFrom(data));
            logEntryPO.build();
            return null;
        }
    }

}
