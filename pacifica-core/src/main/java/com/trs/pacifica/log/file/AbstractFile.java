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

package com.trs.pacifica.log.file;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractFile {

    private final FileHeader header = new FileHeader();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();

    private final String filename;

    public AbstractFile(String filename) {
        this.filename = filename;
    }


    public String getFilename() {
        return this.filename;
    }

    /**
     * 
     * @return
     */
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            return this.header.getFirstLogIndex();
        } finally {
            this.readLock.unlock();
        }

    }

    public long getStartOffset() {
        this.readLock.lock();
        try {
            return this.header.getStartOffset();
        } finally {
            this.readLock.unlock();
        }
    }


}
