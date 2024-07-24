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

import com.trs.pacifica.log.store.IndexStore;
import com.trs.pacifica.log.store.SegmentStore;

public class FsLogStorageOption {

    private int segmentFileSize = SegmentStore._DEFAULT_SEGMENT_FILE_SIZE;

    private int indexEntryCountPerFile = IndexStore._DEFAULT_INDEX_ENTRY_COUNT_PER_FILE;


    private String indexDirName = IndexStore._DEFAULT_INDEX_DIR_NAME;

    private String segmentDirName = SegmentStore._DEFAULT_SEGMENT_DIR_NAME;

    public int getSegmentFileSize() {
        return segmentFileSize;
    }

    public int getIndexEntryCountPerFile() {
        return indexEntryCountPerFile;
    }

    public void setSegmentFileSize(int segmentFileSize) {
        this.segmentFileSize = segmentFileSize;
    }

    public void setIndexEntryCountPerFile(int indexEntryCountPerFile) {
        this.indexEntryCountPerFile = indexEntryCountPerFile;
    }


    public String getIndexDirName() {
        return indexDirName;
    }

    public void setIndexDirName(String indexDirName) {
        this.indexDirName = indexDirName;
    }

    public String getSegmentDirName() {
        return segmentDirName;
    }

    public void setSegmentDirName(String segmentDirName) {
        this.segmentDirName = segmentDirName;
    }
}
