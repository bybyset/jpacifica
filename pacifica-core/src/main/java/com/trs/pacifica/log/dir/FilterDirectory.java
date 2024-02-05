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

package com.trs.pacifica.log.dir;

import com.trs.pacifica.log.error.AlreadyClosedException;
import com.trs.pacifica.log.io.InOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public abstract class FilterDirectory extends Directory {


    protected final Directory delegate;

    protected FilterDirectory(Directory delegate) {
        this.delegate = delegate;
    }

    /**
     * Return the wrapped {@link Directory}.
     */
    public final Directory getDelegate() {
        return delegate;
    }

    @Override
    public String[] listAll() throws IOException {
        return this.delegate.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        this.delegate.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return this.delegate.fileLength(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        this.delegate.sync(names);
    }

    @Override
    public InOutput openInOutput(String name) throws IOException {
        return this.delegate.openInOutput(name);
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return this.delegate.getPendingDeletions();
    }

    @Override
    protected void ensureOpen() throws AlreadyClosedException {
        this.delegate.ensureOpen();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + delegate.toString() + ")";
    }

    /**
     * Get the wrapped instance by <code>dir</code> as long as this reader is an instance of {@link
     * FilterDirectory}.
     */
    public static Directory unwrap(Directory dir) {
        while (dir instanceof FilterDirectory) {
            dir = ((FilterDirectory) dir).delegate;
        }
        return dir;
    }


}
