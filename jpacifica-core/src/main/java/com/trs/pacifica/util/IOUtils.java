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

package com.trs.pacifica.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;

public class IOUtils {

    static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils() {
    }

    /**
     * Closes all given <code>Closeable</code>s. Some of the <code>Closeable</code>s may be null; they
     * are ignored. After everything is closed, the method either throws the first exception it hit
     * while closing, or completes normally if there were no exceptions.
     *
     * @param objects objects to call <code>close()</code> on
     */
    public static void close(Closeable... objects) throws IOException {
        close(Arrays.asList(objects));
    }

    /**
     * Closes all given <code>Closeable</code>s.
     *
     * @see #close(Closeable...)
     */
    public static void close(Iterable<? extends Closeable> objects) throws IOException {
        Throwable th = null;
        for (Closeable object : objects) {
            try {
                if (object != null) {
                    object.close();
                }
            } catch (Throwable t) {
                th = useOrSuppress(th, t);
            }
        }

        if (th != null) {
            throw rethrowAlways(th);
        }
    }

    /**
     * Closes all given <code>Closeable</code>s, suppressing all thrown exceptions. Some of the <code>
     * Closeable</code>s may be null, they are ignored.
     *
     * @param objects objects to call <code>close()</code> on
     */
    public static void closeWhileHandlingException(Closeable... objects) {
        closeWhileHandlingException(Arrays.asList(objects));
    }

    /**
     * Closes all given <code>Closeable</code>s, suppressing all thrown non {@link
     * VirtualMachineError} exceptions. Even if a {@link VirtualMachineError} is thrown all given
     * closeable are closed.
     *
     * @see #closeWhileHandlingException(Closeable...)
     */
    public static void closeWhileHandlingException(Iterable<? extends Closeable> objects) {
        VirtualMachineError firstError = null;
        Throwable firstThrowable = null;
        for (Closeable object : objects) {
            try {
                if (object != null) {
                    object.close();
                }
            } catch (VirtualMachineError e) {
                firstError = useOrSuppress(firstError, e);
            } catch (Throwable t) {
                firstThrowable = useOrSuppress(firstThrowable, t);
            }
        }
        if (firstError != null) {
            // we ensure that we bubble up any errors. We can't recover from these but need to make sure
            // they are
            // bubbled up. if a non-VMError is thrown we also add the suppressed exceptions to it.
            if (firstThrowable != null) {
                firstError.addSuppressed(firstThrowable);
            }
            throw firstError;
        }
    }

    /**
     * Returns the second throwable if the first is null otherwise adds the second as suppressed to
     * the first and returns it.
     */
    public static <T extends Throwable> T useOrSuppress(T first, T second) {
        if (first == null) {
            return second;
        } else {
            first.addSuppressed(second);
        }
        return first;
    }

    /**
     * This utility method takes a previously caught (non-null) {@code Throwable} and rethrows either
     * the original argument if it was a subclass of the {@code IOException} or an {@code
     * RuntimeException} with the cause set to the argument.
     *
     * <p>This method <strong>never returns any value</strong>, even though it declares a return value
     * of type {@link Error}. The return value declaration is very useful to let the compiler know
     * that the code path following the invocation of this method is unreachable. So in most cases the
     * invocation of this method will be guarded by an {@code if} and used together with a {@code
     * throw} statement, as in:
     *
     * <pre>{@code
     * if (t != null) throw IOUtils.rethrowAlways(t)
     * }</pre>
     *
     * @param th The throwable to rethrow, <strong>must not be null</strong>.
     * @return This method always results in an exception, it never returns any value. See method
     * documentation for details and usage example.
     * @throws IOException      if the argument was an instance of IOException
     * @throws RuntimeException with the {@link RuntimeException#getCause()} set to the argument, if
     *                          it was not an instance of IOException.
     */
    public static Error rethrowAlways(Throwable th) throws IOException, RuntimeException {
        if (th == null) {
            throw new AssertionError("rethrow argument must not be null.");
        }

        if (th instanceof IOException) {
            throw (IOException) th;
        }

        if (th instanceof RuntimeException) {
            throw (RuntimeException) th;
        }

        if (th instanceof Error) {
            throw (Error) th;
        }

        throw new RuntimeException(th);
    }

    /**
     * Ensure that any writes to the given file is written to the storage device that contains it.
     *
     * @param fileToSync the file to fsync
     * @param isDir      if true, the given file is a directory (we open for read and ignore IOExceptions,
     *                   because not all file systems and operating systems allow to fsync on a directory)
     */
    public static void fsync(Path fileToSync, boolean isDir) throws IOException {
        // If the file is a directory we have to open read-only, for regular files we must open r/w for
        // the fsync to have an effect.
        // See http://blog.httrack.com/blog/2013/11/15/everything-you-always-wanted-to-know-about-fsync/
        if (isDir && SystemConstants.WINDOWS) {
            // opening a directory on Windows fails, directories can not be fsynced there
            if (Files.exists(fileToSync) == false) {
                // yet do not suppress trying to fsync directories that do not exist
                throw new NoSuchFileException(fileToSync.toString());
            }
            return;
        }
        try (final FileChannel file =
                     FileChannel.open(fileToSync, isDir ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
            try {
                file.force(true);
            } catch (final IOException e) {
                if (isDir) {
                    assert (SystemConstants.LINUX || SystemConstants.MAC_OS_X) == false
                            : "On Linux and MacOSX fsyncing a directory should not throw IOException, "
                            + "we just don't want to rely on that in production (undocumented). Got: "
                            + e;
                    // Ignore exception if it is a directory
                    return;
                }
                // Throw original exception
                throw e;
            }
        }
    }


    public static boolean atomicMoveFile(final File source, final File target, final boolean sync) throws IOException {
        final Path sourcePath = source.toPath();
        final Path targetPath = target.toPath();
        boolean success;
        try {
            success = Files.move(sourcePath, targetPath, StandardCopyOption.ATOMIC_MOVE) != null;
        } catch (final IOException e) {
            // If it falls here that can mean many things. Either that the atomic move is not supported,
            // or something wrong happened. Anyway, let's try to be over-diagnosing
            if (e instanceof AtomicMoveNotSupportedException) {
                LOGGER.warn("Atomic move not supported, falling back to non-atomic move, error: {}.", e.getMessage());
            } else {
                LOGGER.error("Unable to move atomically, falling back to non-atomic move, error: {}.", e);
            }

            if (target.exists()) {
                LOGGER.info("The target file {} was already existing.", targetPath);
            }

            try {
                success = Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING) != null;
            } catch (final IOException e1) {
                e1.addSuppressed(e);
                LOGGER.error("Unable to move {} to {}. Attempting to delete {} and abandoning.", sourcePath, targetPath,
                        sourcePath, e1);
                try {
                    Files.deleteIfExists(sourcePath);
                } catch (final IOException e2) {
                    e2.addSuppressed(e1);
                    LOGGER.error("Unable to delete {}, good bye then!", sourcePath, e2);
                    throw e2;
                }

                throw e1;
            }
        }

        if (success && sync) {
            // fsync on target parent dir.
            fsync(targetPath, target.isDirectory());
        }
        return true;
    }
}
