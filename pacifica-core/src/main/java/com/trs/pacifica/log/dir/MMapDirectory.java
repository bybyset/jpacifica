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

import com.trs.pacifica.log.io.Input;
import com.trs.pacifica.log.io.Output;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.file.Path;
import java.util.Collection;
import java.util.logging.Logger;

public class MMapDirectory extends FsDirectory {

    /**
     * <code>true</code>, if this platform supports unmapping mmapped files.
     */
    public static final boolean UNMAP_SUPPORTED;


    /**
     * if {@link #UNMAP_SUPPORTED} is {@code false}, this contains the reason why unmapping is not
     * supported.
     */
    public static final String UNMAP_NOT_SUPPORTED_REASON;

    static final MMapInputProvider PROVIDER;


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


    public MMapDirectory(Path path) throws IOException {
        super(path);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }

    @Override
    public Input openInput(String name) throws IOException {
        return null;
    }

    @Override
    public Output openOutput(String name) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    private static MMapInputProvider lookupProvider() {
        final var lookup = MethodHandles.lookup();
        try {
            final var cls = lookup.findClass("org.apache.lucene.store.MemorySegmentIndexInputProvider");
            // we use method handles, so we do not need to deal with setAccessible as we have private
            // access through the lookup:
            final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
            try {
                return (MMapInputProvider) constr.invoke();
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

    static interface MMapInputProvider {

        boolean isUnmapSupported();

        String getUnmapNotSupportedReason();

        long getDefaultMaxChunkSize();

    }
}
