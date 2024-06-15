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

import com.trs.pacifica.log.io.InOutput;
import com.trs.pacifica.log.io.Input;
import com.trs.pacifica.log.io.MappedByteBufferInputProvider;
import com.trs.pacifica.util.OnlyForTest;
import com.trs.pacifica.util.SystemConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * code from lucene
 */
public class MMapDirectory extends FsDirectory {

    private static final Logger LOGGER = LoggerFactory.getLogger(MMapDirectory.class);

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
     */
    public static final long DEFAULT_MAX_CHUNK_SIZE;


    static {
        PROVIDER = lookupProvider();
        DEFAULT_MAX_CHUNK_SIZE = PROVIDER.getDefaultMaxChunkSize();
        UNMAP_SUPPORTED = PROVIDER.isUnmapSupported();
        UNMAP_NOT_SUPPORTED_REASON = PROVIDER.getUnmapNotSupportedReason();
    }

    private final int chunkSizePower;
    private Predicate<String> preload = NO_FILES;
    private boolean useUnmapHack = UNMAP_SUPPORTED;

    private Map<String, InOutput> fileInOutputCache = new ConcurrentHashMap<>();


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
    public InOutput openInOutput(String name) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        final InOutput cache = fileInOutputCache.computeIfAbsent(name, (key) -> {
            try {
                Path path = getDirectory().resolve(name);
                return PROVIDER.openInput(path, chunkSizePower, preload.test(name), useUnmapHack);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return cache.clone();
    }


    @OnlyForTest
    InOutput getFileInOutput(final String filename) {
        return this.fileInOutputCache.get(filename);
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        this.fileInOutputCache.forEach((name, inOutput) -> {
            try {
                inOutput.close();
            } catch (IOException e) {
                LOGGER.error("{} failed to close InOutput of file={}.", getDirectory(), name);
            }
        });
    }

    @Override
    protected void onDeleteFileBefore(String filename) {
        final InOutput inOutput = this.fileInOutputCache.remove(filename);
        if (inOutput != null) {
            try {
                inOutput.close();
            } catch (IOException e){
                //
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error("Failed to close InOutput(dir={}, filename={})", this.getDirectory(), filename);
                }
            }
        }
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


    /**
     * This method enables the workaround for unmapping the buffers from address space after closing
     * {@link Input}, that is mentioned in the bug report. This hack may fail on
     * non-Oracle/OpenJDK JVMs. It forcefully unmaps the buffer on close by using an undocumented
     * internal cleanup functionality.
     *
     * <p>On Java 19 with {@code --enable-preview} command line setting, this class will use the
     * modern {@code MemorySegment} API which allows to safely unmap. <em>The following warnings no
     * longer apply in that case!</em>
     *
     * <p><b>NOTE:</b> Enabling this is completely unsupported by Java and may lead to JVM crashes if
     * <code>IndexInput</code> is closed while another thread is still accessing it (SIGSEGV).
     *
     * <p>To enable the hack, the following requirements need to be fulfilled: The used JVM must be
     * Oracle Java / OpenJDK 8 <em>(preliminary support for Java 9 EA build 150+ was added with Lucene
     * 6.4)</em>. In addition, the following permissions need to be granted to {@code lucene-core.jar}
     * in your <a
     * href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/PolicyFiles.html">policy
     * file</a>:
     *
     * <ul>
     *   <li>{@code permission java.lang.reflect.ReflectPermission "suppressAccessChecks";}
     *   <li>{@code permission java.lang.RuntimePermission "accessClassInPackage.sun.misc";}
     * </ul>
     *
     * @throws IllegalArgumentException if {@link #UNMAP_SUPPORTED} is <code>false</code> and the
     *                                  workaround cannot be enabled. The exception message also contains an explanation why the
     *                                  hack cannot be enabled (e.g., missing permissions).
     */
    public void setUseUnmap(final boolean useUnmapHack) {
        if (useUnmapHack && !UNMAP_SUPPORTED) {
            throw new IllegalArgumentException(UNMAP_NOT_SUPPORTED_REASON);
        }
        this.useUnmapHack = useUnmapHack;
    }

    /**
     * Returns <code>true</code>, if the unmap workaround is enabled.
     *
     * @see #setUseUnmap
     */
    public boolean getUseUnmap() {
        return useUnmapHack;
    }

    private static MMapInOutputProvider lookupProvider() {
        return new MappedByteBufferInputProvider();
    }


    public static interface MMapInOutputProvider {

        /**
         *
         * @param path
         * @param chunkSizePower
         * @param preload
         * @param useUnmapHack
         * @return
         * @throws IOException
         */
        InOutput openInput(Path path, int chunkSizePower, boolean preload, boolean useUnmapHack) throws IOException;

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
            if (!SystemConstants.JRE_IS_64BIT) {
                moreInfo =
                        "MMapDirectory should only be used on 64bit platforms, because the address space on 32bit operating systems is too small. ";
            } else if (SystemConstants.WINDOWS) {
                moreInfo =
                        "Windows is unfortunately very limited on virtual address space. If your index size is several hundred Gigabytes, consider changing to Linux. ";
            } else if (SystemConstants.LINUX) {
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
