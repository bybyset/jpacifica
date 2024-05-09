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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class Directory implements Closeable {

    /**
     * Returns names of all files stored in this directory.
     *
     * @return
     * @throws IOException in case of I/O error
     */
    public abstract String[] listAll() throws IOException;


    /**
     * Removes an existing file in the directory.
     * <p>This method must throw either {@link NoSuchFileException} or {@link FileNotFoundException}
     * if {@code name} points to a non-existing file.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    public abstract void deleteFile(String name) throws IOException;


    /**
     * Returns the byte length of a file in the directory.
     * <p>This method must throw either {@link NoSuchFileException} or {@link FileNotFoundException}
     * if {@code name} points to a non-existing file.
     *
     * @param name
     * @return
     * @throws IOException
     */
    public abstract int fileLength(String name) throws IOException;


    /**
     * Ensures that any writes to these files are moved to stable storage (made durable).
     *
     * @param names
     * @throws IOException
     */
    public abstract void sync(Collection<String> names) throws IOException;

    public void sync(String filename) throws IOException {
        sync(List.of(filename));
    }


    /**
     * Opens a stream for reading an existing file.
     * Opens a stream for appending an existing file.
     * <p>This method must throw either {@link NoSuchFileException} or {@link FileNotFoundException}
     * if {@code name} points to a non-existing file.
     *
     * @param name name the name of an existing file.
     * @return Input
     * @throws IOException in case of I/O error
     */
    public abstract InOutput openInOutput(String name) throws IOException;


    /**
     * Creates a file with the specified filename and file byte size
     * @param filename name of the file
     * @param fileSize byte size of the file
     * @throws IOException
     */
    public abstract void createFile(String filename, int fileSize) throws IOException;


    /**
     * Ensures this directory is still open.
     *
     * @throws AlreadyClosedException if this directory is closed.
     */
    protected void ensureOpen() throws AlreadyClosedException {
    }

    /**
     * Returns a set of files currently pending deletion in this directory.
     *
     * @lucene.internal
     */
    public abstract Set<String> getPendingDeletions() throws IOException;
}
