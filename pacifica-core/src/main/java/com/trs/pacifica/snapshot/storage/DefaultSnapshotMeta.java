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

package com.trs.pacifica.snapshot.storage;

import com.trs.pacifica.model.LogId;
import com.trs.pacifica.util.BitUtil;
import com.trs.pacifica.util.IOUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultSnapshotMeta {

    static final String SNAPSHOT_META_FILE = "_snapshot_meta";
    static final String TEMP_SUFFIX = ".temp";

    private final LogId snapshotLogId;
    private final Map<String, Object> files = new ConcurrentHashMap<>();

    DefaultSnapshotMeta(final LogId snapshotLogId) {
        this.snapshotLogId = snapshotLogId;
    }

    public boolean addFile(String filename, Object fileMeta) {
        return this.files.putIfAbsent(filename, fileMeta) != null;
    }

    public boolean removeFile(String filename) {
        return this.files.remove(filename) != null;
    }

    public LogId getSnapshotLogId() {
        return this.snapshotLogId;
    }

    public Collection<String> listFiles() {
        return this.files.keySet();
    }


    public static DefaultSnapshotMeta newSnapshotMeta(LogId snapshotLogId) {
        return new DefaultSnapshotMeta(snapshotLogId);
    }

    public static DefaultSnapshotMeta loadFromFile(String filePath) throws IOException {
        File saveTempFile = new File(filePath);
        if (saveTempFile.exists()) {
            try (final FileInputStream fis = new FileInputStream(saveTempFile)){
                final byte[] bytes = fis.readAllBytes();
                return decode(bytes);
            }
        }
        return null;
    }

    public static void saveToFile(DefaultSnapshotMeta snapshotMeta, String filePath, boolean sync) throws IOException {
        File saveTempFile = new File(filePath + TEMP_SUFFIX);
        if (saveTempFile.exists()) {
            FileUtils.forceDelete(saveTempFile);
        }
        try (final FileOutputStream fos = new FileOutputStream(saveTempFile)) {
            List<byte[]> bytes = encode(snapshotMeta);
            for (byte[] b : bytes) {
                fos.write(b);
            }
        }

        if (sync) {
            IOUtils.fsync(saveTempFile.toPath(), false);
        }
        IOUtils.atomicMoveFile(saveTempFile, new File(filePath) , sync);

    }

    private static final byte[] HEADER = new byte[8];

    static List<byte[]> encode(DefaultSnapshotMeta snapshotMeta) {
        List<byte[]> bytes = new ArrayList<>();
        bytes.add(HEADER);
        final long snapshotLogIndex = snapshotMeta.snapshotLogId.getIndex();
        byte[] snapshotLogIndexBytes = new byte[Long.BYTES];
        BitUtil.putLong(snapshotLogIndexBytes, 0 , snapshotLogIndex);
        bytes.add(snapshotLogIndexBytes);
        final long snapshotLogTerm = snapshotMeta.snapshotLogId.getTerm();
        byte[] snapshotLogTermBytes = new byte[Long.BYTES];
        BitUtil.putLong(snapshotLogTermBytes, 0 , snapshotLogTerm);
        bytes.add(snapshotLogTermBytes);
        //
        int size = snapshotMeta.files.size();
        byte[] fileSizeBytes = new byte[Integer.BYTES];
        BitUtil.putInt(fileSizeBytes, 0 , size);
        bytes.add(fileSizeBytes);
        //
        snapshotMeta.files.forEach((filename, meta) -> {
            byte[] utf8Filename = filename.getBytes(StandardCharsets.UTF_8);
            byte[] utf8FilenameLenBytes = new byte[Integer.BYTES];
            BitUtil.putInt(utf8FilenameLenBytes, 0 , utf8Filename.length);
            bytes.add(utf8FilenameLenBytes);
            bytes.add(utf8Filename);
        });
        return bytes;
    }

    static DefaultSnapshotMeta decode(byte[] bytes) {
        int offset = HEADER.length;
        final long snapshotLogIndex = BitUtil.getLong(bytes, offset);
        offset += Long.BYTES;
        final long snapshotLogTerm = BitUtil.getLong(bytes, offset);
        final LogId snapshotLogId = new LogId(snapshotLogIndex, snapshotLogTerm);
        offset += Long.BYTES;
        DefaultSnapshotMeta meta = new DefaultSnapshotMeta(snapshotLogId);
        final int fileSize = BitUtil.getInt(bytes, offset);
        offset += Integer.BYTES;

        for (int i = 0; i < fileSize; i++) {
            final int utf8FilenameLen = BitUtil.getInt(bytes, offset);
            offset += Integer.BYTES;
            final String filename = new String(bytes, offset, utf8FilenameLen);
            offset += utf8FilenameLen;
            meta.addFile(filename, "");
        }
        return meta;
    }


    static String getSnapshotMetaFilePath(final String snapshotDirectory) {
        return snapshotDirectory + File.separator + SNAPSHOT_META_FILE;
    }

}
