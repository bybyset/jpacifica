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

import com.trs.pacifica.util.SystemConstants;
import com.trs.pacifica.util.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FsDirectory extends BaseDirectory {

    protected final Path directory;


    private final AtomicInteger opsSinceLastDelete = new AtomicInteger();

    /**
     * Maps files that we are trying to delete (or we tried already but failed) before attempting to delete that key.
     */
    private final Set<String> pendingDeletes =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public FsDirectory(Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            Files.createDirectories(path); // create directory, if it doesn't exist
        }
        directory = path.toRealPath();
    }


    /**
     *
     */
    public static FsDirectory open(Path path) throws IOException {
        if (SystemConstants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
            return new MMapDirectory(path);
        } else {
            return new NIOFSDirectory(path);
        }
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return listAll(directory, null);
    }

    private static String[] listAll(Path dir, Set<String> skipNames) throws IOException {
        List<String> entries = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (skipNames == null || skipNames.contains(name) == false) {
                    entries.add(name);
                }
            }
        }
        String[] array = entries.toArray(new String[entries.size()]);
        Arrays.sort(array);
        return array;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (pendingDeletes.contains(name)) {
            throw new NoSuchFileException("file \"" + name + "\" is already pending delete");
        }
        privateDeleteFile(name, false);
        maybeDeletePendingFiles();
    }

    @Override
    public int fileLength(String name) throws IOException {
        ensureOpen();
        if (pendingDeletes.contains(name)) {
            throw new NoSuchFileException("file \"" + name + "\" is pending delete");
        }
        return (int) Files.size(directory.resolve(name));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        for (String name : names) {
            fsync(name);
        }
        maybeDeletePendingFiles();
    }

    @Override
    public synchronized Set<String> getPendingDeletions() throws IOException {
        deletePendingFiles();
        if (pendingDeletes.isEmpty()) {
            return Collections.emptySet();
        } else {
            return Set.copyOf(pendingDeletes);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
        deletePendingFiles();
    }

    @Override
    public void createFile(String filename, int fileSize) throws IOException {
        final Path path = this.directory.resolve(filename);
        File file = path.toFile();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");) {
            randomAccessFile.setLength(fileSize);
        }
    }

    protected void ensureCanRead(String name) throws IOException {
        if (pendingDeletes.contains(name)) {
            throw new NoSuchFileException(
                    "file \"" + name + "\" is pending delete and cannot be opened for read");
        }
    }

    protected void fsync(String name) throws IOException {
        IOUtils.fsync(directory.resolve(name), false);
    }

    /**
     * Try to delete any pending files that we had previously tried to delete but failed because we
     * are on Windows and the files were still held open.
     */
    public synchronized void deletePendingFiles() throws IOException {
        if (pendingDeletes.isEmpty() == false) {

            // TODO: we could fix IndexInputs from FSDirectory subclasses to call this when they are
            // closed?
            // Clone the set since we mutate it in privateDeleteFile:
            for (String name : new HashSet<>(pendingDeletes)) {
                privateDeleteFile(name, true);
            }
        }
    }

    private void maybeDeletePendingFiles() throws IOException {
        if (pendingDeletes.isEmpty() == false) {
            // This is a silly heuristic to try to avoid O(N^2), where N = number of files pending
            // deletion, behaviour on Windows:
            int count = opsSinceLastDelete.incrementAndGet();
            if (count >= pendingDeletes.size()) {
                opsSinceLastDelete.addAndGet(-count);
                deletePendingFiles();
            }
        }
    }

    private void privateDeleteFile(String name, boolean isPendingDelete) throws IOException {
        try {
            Files.delete(directory.resolve(name));
            pendingDeletes.remove(name);
        } catch (NoSuchFileException | FileNotFoundException e) {
            // We were asked to delete a non-existent file:
            pendingDeletes.remove(name);
            if (isPendingDelete && SystemConstants.WINDOWS) {
                // TODO: can we remove this OS-specific hacky logic?  If windows deleteFile is buggy, we
                // should instead contain this workaround in
                // a WindowsFSDirectory ...
                // "pending delete" state, failing the first
                // delete attempt with access denied and then apparently falsely failing here when we try ot
                // delete it again, with NSFE/FNFE
            } else {
                throw e;
            }
        } catch (
                @SuppressWarnings("unused")
                IOException ioe) {
            // On windows, a file delete can fail because there's still an open
            // file handle against it.  We record this in pendingDeletes and
            // try again later.

            // TODO: this is hacky/lenient (we don't know which IOException this is), and
            // it should only happen on filesystems that can do this, so really we should
            // move this logic to WindowsDirectory or something

            // TODO: can/should we do if (Constants.WINDOWS) here, else throw the exc?
            // but what about a Linux box with a CIFS mount?
            pendingDeletes.add(name);
        }
    }

}
