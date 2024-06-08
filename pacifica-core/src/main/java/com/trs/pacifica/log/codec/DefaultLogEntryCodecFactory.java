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
import com.trs.pacifica.util.io.ByteDataBuffer;
import com.trs.pacifica.util.io.DataBuffer;
import com.trs.pacifica.util.io.DataBufferInputStream;
import com.trs.pacifica.util.io.LinkedDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

@SPI
public class DefaultLogEntryCodecFactory implements LogEntryCodecFactory {

    static final Logger LOGGER = LoggerFactory.getLogger(DefaultLogEntryCodecFactory.class);

    // magic
    public static final byte[] MAGIC_BYTES = new byte[]{(byte) 0xBB, (byte) 0xD2};
    // Codec version
    public static final byte CURRENT_VERSION = 0x01;
    // reserved bytes
    public static final byte[] RESERVED = new byte[3];

    static final int HEADER_BYTES = MAGIC_BYTES.length + RESERVED.length + 1;

    static final LogEntryDecoder PROTOBUF_DECODER = new ProtobufLogEntryDecoder();

    static final LogEntryEncoder PROTOBUF_ENCODER = new ProtobufLogEntryEncoder();

    private static final byte[] CURRENT_LOG_ENTRY_HEADER_BYTES = new LogEntryHeader(MAGIC_BYTES, CURRENT_VERSION).encode();

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
            final byte[] headerBytes = new byte[HEADER_BYTES];
            data.get(headerBytes);
            final byte[] magic = new byte[MAGIC_BYTES.length];
            int i = 0;
            for (; i < magic.length; i++) {
                magic[i] = headerBytes[i];
            }
            if (!Arrays.equals(magic, MAGIC_BYTES)) {
                return null;
            }
            final byte version = headerBytes[i++];
            //
            try (InputStream inputStream = new DataBufferInputStream(data)) {
                RpcCommon.LogEntryPO logEntryPO = RpcCommon.LogEntryPO.parseFrom(inputStream);
                final long logIndex = logEntryPO.getLogIndex();
                final long logTerm = logEntryPO.getLogTerm();
                LogEntry.Type type = RpcUtil.toLogEntryType(logEntryPO.getType());
                final LogEntry logEntry = new LogEntry(logIndex, logTerm, type);
                if (logEntryPO.hasData()) {
                    ByteBuffer logData = logEntryPO.getData().asReadOnlyByteBuffer();
                    logEntry.setLogData(logData);
                }
                if (logEntryPO.hasChecksum()) {
                    logEntry.setChecksum(logEntryPO.getChecksum());
                }
                return logEntry;
            } catch (IOException e) {
                LOGGER.error("failed to decode LogEntry", e);
                return null;
            }
        }
    }

    static class ProtobufLogEntryEncoder implements LogEntryEncoder {

        @Override
        public DataBuffer encode(LogEntry logEntry) {
            // header
            final byte[] headerBytes = encodeHeader();
            // body
            RpcCommon.LogEntryPO.Builder logEntryPO = RpcCommon.LogEntryPO.newBuilder();
            logEntryPO.setType(RpcUtil.protoLogEntryType(logEntry.getType()));
            logEntryPO.setLogIndex(logEntry.getLogId().getIndex());
            logEntryPO.setLogTerm(logEntry.getLogId().getTerm());
            final ByteBuffer data = logEntry.getLogData();
            logEntryPO.setData(ByteString.copyFrom(data));
            if (logEntry.hasChecksum()) {
                logEntryPO.setChecksum(logEntry.getChecksum());
            }
            //
            final DataBuffer header = new ByteDataBuffer(headerBytes);
            final DataBuffer body = new ByteDataBuffer(logEntryPO.build().toByteArray());
            DataBuffer encode = new LinkedDataBuffer(header, body);
            return encode;
        }
    }


    private static class LogEntryHeader {
        private byte[] magic = MAGIC_BYTES;
        private byte version = CURRENT_VERSION;

        LogEntryHeader(byte[] magic, byte version) {
            this.magic = magic;
            this.version = version;
        }

        LogEntryHeader() {
        }


        byte[] encode() {
            byte[] header = new byte[HEADER_BYTES];
            int i = 0;
            for (; i < MAGIC_BYTES.length; i++) {
                header[i] = MAGIC_BYTES[i];
            }
            header[i++] = CURRENT_VERSION;
            for (; i < HEADER_BYTES; i++) {
                header[i] = RESERVED[i - MAGIC_BYTES.length - 1];
            }
            return header;
        }


    }

    static final byte[] encodeHeader() {
        return CURRENT_LOG_ENTRY_HEADER_BYTES;
    }


}
