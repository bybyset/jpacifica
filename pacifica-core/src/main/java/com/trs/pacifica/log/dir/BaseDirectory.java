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
import com.trs.pacifica.log.io.Input;
import com.trs.pacifica.log.io.Output;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;

public abstract class BaseDirectory implements Closeable {


    protected volatile boolean isOpen = true;




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
    public abstract long fileLength(String name) throws IOException;


    /**
     * Ensures that any writes to these files are moved to stable storage (made durable).
     *
     * @param names
     * @throws IOException
     */
    public abstract void sync(Collection<String> names) throws IOException;


    /**
     * Opens a stream for reading an existing file.
     * <p>This method must throw either {@link NoSuchFileException} or {@link FileNotFoundException}
     * if {@code name} points to a non-existing file.
     *
     * @param name name the name of an existing file.
     * @return Input
     * @throws IOException in case of I/O error
     */
    public abstract Input openInput(String name) throws IOException;

    /**
     * Creates a new, empty file in the directory and returns an {@link Output} instance for appending data to this file.
     * <p>This method must throw {@link FileAlreadyExistsException} if the file already exists.
     *
     * @param name name the name of the file to create.
     * @return
     * @throws IOException in case of I/O error
     */
    public abstract Output openOutput(String name) throws IOException;

    protected final void ensureOpen() throws AlreadyClosedException {
        if (!isOpen) {
            throw new AlreadyClosedException("this Directory is closed");
        }
    }

}
