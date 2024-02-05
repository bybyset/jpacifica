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

import com.trs.pacifica.log.io.DataInOutput;
import com.trs.pacifica.util.Constants;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.file.Path;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.logging.Logger;

public class MMapDirectory extends FsDirectory {

    /**
     * Argument for {@link #setPreload(Predicate)} that configures all files to be preloaded upon
     * opening them.
     */
    public static final Predicate<String> ALL_FILES = (filename) -> true;

    /**
     * Argument for {@link #setPreload(Predicate)} that configures no files to be preloaded upon
     * opening them.
     */
    public static final Predicate<String> NO_FILES = (filename) -> false;

    /**
     * <code>true</code>, if this platform supports unmapping mmapped files.
     */
    public static final boolean UNMAP_SUPPORTED;


    /**
     * if {@link #UNMAP_SUPPORTED} is {@code false}, this contains the reason why unmapping is not
     * supported.
     */
    public static final String UNMAP_NOT_SUPPORTED_REASON;

    static final MMapInOutputProvider PROVIDER;


    /**
     * Default max chunk size:
     *
     * <ul>
     *   <li>16 GiBytes for 64 bit <b>Java 19</b> JVMs running with {@code --enable-preview} as
     *       command line parameter
     *   <li>1 GiBytes for other 64 bit JVMs
     *   <li>256 MiBytes for 32 bit JVMs
     * </ul>
     */
    public static final long DEFAULT_MAX_CHUNK_SIZE;


    private final int chunkSizePower;

    private Predicate<String> preload = NO_FILES;


    public MMapDirectory(Path path) throws IOException {
        this(path, DEFAULT_MAX_CHUNK_SIZE);
    }

    public MMapDirectory(Path path, long maxChunkSize) throws IOException {
        super(path);
        if (maxChunkSize <= 0L) {
            throw new IllegalArgumentException("Maximum chunk size for mmap must be >0");
        }
        this.chunkSizePower = Long.SIZE - 1 - Long.numberOfLeadingZeros(maxChunkSize);
        assert (1L << chunkSizePower) <= maxChunkSize;
        assert (1L << chunkSizePower) > (maxChunkSize / 2);
    }


    @Override
    public DataInOutput openInOutput(String name) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        Path path = directory.resolve(name);
        return PROVIDER.openInput(path, chunkSizePower, preload.test(name));
    }

    /**
     * Configure which files to preload in physical memory upon opening. The default implementation
     * does not preload anything. The behavior is best effort and operating system-dependent.
     *
     * @param preload a {@link Predicate} whose first argument is the file name
     * @see #ALL_FILES
     * @see #NO_FILES
     */
    public void setPreload(Predicate<String> preload) {
        this.preload = preload;
    }

    private static MMapInOutputProvider lookupProvider() {
        final var lookup = MethodHandles.lookup();
        try {
            final var cls = lookup.findClass("org.apache.lucene.store.MemorySegmentIndexInputProvider");
            // we use method handles, so we do not need to deal with setAccessible as we have private
            // access through the lookup:
            final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
            try {
                return (MMapInOutputProvider) constr.invoke();
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Throwable th) {
                throw new AssertionError(th);
            }
        } catch (
                @SuppressWarnings("unused")
                ClassNotFoundException e) {
            // we're before Java 19
            return new MappedByteBufferInputProvider();
        } catch (
                @SuppressWarnings("unused")
                UnsupportedClassVersionError e) {
            var log = Logger.getLogger(lookup.lookupClass().getName());
            if (Runtime.version().feature() == 19) {
                log.warning(
                        "You are running with Java 19. To make full use of MMapDirectory, please pass '--enable-preview' to the Java command line.");
            } else {
                log.warning(
                        "You are running with Java 20 or later. To make full use of MMapDirectory, please update Apache Lucene.");
            }
            return new MappedByteBufferInputProvider();
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new LinkageError(
                    "MemorySegmentIndexInputProvider is missing correctly typed constructor", e);
        }
    }

    static {
        PROVIDER = lookupProvider();
        DEFAULT_MAX_CHUNK_SIZE = PROVIDER.getDefaultMaxChunkSize();
        UNMAP_SUPPORTED = PROVIDER.isUnmapSupported();
        UNMAP_NOT_SUPPORTED_REASON = PROVIDER.getUnmapNotSupportedReason();
    }

    static interface MMapInOutputProvider {

        DataInOutput openInput(Path path, int chunkSizePower, boolean preload) throws IOException;

        boolean isUnmapSupported();

        String getUnmapNotSupportedReason();

        long getDefaultMaxChunkSize();

        default IOException convertMapFailedIOException(
                IOException ioe, String resourceDescription, long bufSize) {
            final String originalMessage;
            final Throwable originalCause;
            if (ioe.getCause() instanceof OutOfMemoryError) {
                // nested OOM confuses users, because it's "incorrect", just print a plain message:
                originalMessage = "Map failed";
                originalCause = null;
            } else {
                originalMessage = ioe.getMessage();
                originalCause = ioe.getCause();
            }
            final String moreInfo;
            if (!Constants.JRE_IS_64BIT) {
                moreInfo =
                        "MMapDirectory should only be used on 64bit platforms, because the address space on 32bit operating systems is too small. ";
            } else if (Constants.WINDOWS) {
                moreInfo =
                        "Windows is unfortunately very limited on virtual address space. If your index size is several hundred Gigabytes, consider changing to Linux. ";
            } else if (Constants.LINUX) {
                moreInfo =
                        "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'), and 'sysctl vm.max_map_count'. ";
            } else {
                moreInfo = "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'). ";
            }
            final IOException newIoe =
                    new IOException(
                            String.format(
                                    Locale.ENGLISH,
                                    "%s: %s [this may be caused by lack of enough unfragmented virtual address space "
                                            + "or too restrictive virtual memory limits enforced by the operating system, "
                                            + "preventing us to map a chunk of %d bytes. %sMore information: "
                                            + "https://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html]",
                                    originalMessage,
                                    resourceDescription,
                                    bufSize,
                                    moreInfo),
                            originalCause);
            newIoe.setStackTrace(ioe.getStackTrace());
            return newIoe;
        }

    }
}
