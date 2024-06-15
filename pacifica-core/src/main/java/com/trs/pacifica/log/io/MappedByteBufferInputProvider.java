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

package com.trs.pacifica.log.io;

import com.trs.pacifica.log.dir.MMapDirectory;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.logging.Logger;

import com.trs.pacifica.log.io.ByteBufferGuard.BufferCleaner;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

public class MappedByteBufferInputProvider implements MMapDirectory.MMapInOutputProvider {

    private final BufferCleaner cleaner;

    private final boolean unmapSupported;
    private final String unmapNotSupportedReason;

    public MappedByteBufferInputProvider() {
        final Object hack = doPrivileged(MappedByteBufferInputProvider::unmapHackImpl);
        if (hack instanceof BufferCleaner) {
            cleaner = (BufferCleaner) hack;
            unmapSupported = true;
            unmapNotSupportedReason = null;
        } else {
            cleaner = null;
            unmapSupported = false;
            unmapNotSupportedReason = hack.toString();
            Logger.getLogger(getClass().getName()).warning(unmapNotSupportedReason);
        }
    }

    @Override
    public InOutput openInput(Path path, int chunkSizePower, boolean preload, boolean useUnmapHack) throws IOException {
        if (chunkSizePower > 30) {
            throw new IllegalArgumentException(
                    "ByteBufferIndexInput cannot use a chunk size of >1 GiBytes.");
        }

        final String resourceDescription = "ByteBufferInOutput(path=\"" + path.toString() + "\")";
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long fileSize = fc.size();
            return ByteBufferDataInOutput.newInstance(
                    resourceDescription,
                    map(resourceDescription, fc, chunkSizePower, preload, fileSize),
                    fileSize,
                    chunkSizePower,
                    new ByteBufferGuard(resourceDescription, useUnmapHack ? cleaner : null));
        }
    }

    @Override
    public boolean isUnmapSupported() {
        return unmapSupported;
    }

    @Override
    public String getUnmapNotSupportedReason() {
        return unmapNotSupportedReason;
    }

    @Override
    public long getDefaultMaxChunkSize() {
        return 1L << 20; // 1M
    }


    /**
     * Maps a file into a set of buffers
     */
    final ByteBuffer[] map(
            String resourceDescription, FileChannel fc, int chunkSizePower, boolean preload, long length)
            throws IOException {
        if ((length >>> chunkSizePower) >= Integer.MAX_VALUE)
            throw new IllegalArgumentException(
                    "RandomAccessFile too big for chunk size: " + resourceDescription);

        final long chunkSize = 1L << chunkSizePower;

        final int nrBuffers = (int) (length >>> chunkSizePower) + (length & (chunkSize - 1)) > 0 ? 1 : 0;

        final ByteBuffer[] buffers = new ByteBuffer[nrBuffers];

        long startOffset = 0L;
        for (int bufNr = 0; bufNr < nrBuffers; bufNr++) {
            final int bufSize =
                    (int) ((length > (startOffset + chunkSize)) ? chunkSize : (length - startOffset));
            final MappedByteBuffer buffer;
            try {
                buffer = fc.map(FileChannel.MapMode.READ_WRITE, startOffset, bufSize);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException ioe) {
                throw convertMapFailedIOException(ioe, resourceDescription, bufSize);
            }
            if (preload) {
                buffer.load();
            }
            buffers[bufNr] = buffer;
            startOffset += bufSize;
        }

        return buffers;
    }

    private static Object unmapHackImpl() {
        final MethodHandles.Lookup lookup = lookup();
        try {
            // *** sun.misc.Unsafe unmapping (Java 9+) ***
            final Class<?> unsafeClass = lookup.findClass("sun.misc.Unsafe");
            // first check if Unsafe has the right method, otherwise we can give up
            // without doing any security critical stuff:
            final MethodHandle unmapper =
                    lookup.findVirtual(
                            unsafeClass, "invokeCleaner", methodType(void.class, ByteBuffer.class));
            // fetch the unsafe instance and bind it to the virtual MH:
            final Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            final Object theUnsafe = f.get(null);
            return newBufferCleaner(unmapper.bindTo(theUnsafe));
        } catch (SecurityException se) {
            return "Unmapping is not supported, because not all required permissions are given to the Lucene JAR file: "
                    + se
                    + " [Please grant at least the following permissions: RuntimePermission(\"accessClassInPackage.sun.misc\") "
                    + " and ReflectPermission(\"suppressAccessChecks\")]";
        } catch (ReflectiveOperationException | RuntimeException e) {
            final Module module = MappedByteBufferInputProvider.class.getModule();
            final ModuleLayer layer = module.getLayer();
            // classpath / unnamed module has no layer, so we need to check:
            if (layer != null
                    && layer.findModule("jdk.unsupported").map(module::canRead).orElse(false) == false) {
                return "Unmapping is not supported, because Lucene cannot read 'jdk.unsupported' module "
                        + "[please add 'jdk.unsupported' to modular application either by command line or its module descriptor]";
            }
            return "Unmapping is not supported on this platform, because internal Java APIs are not compatible with this Lucene version: "
                    + e;
        }
    }

    private static ByteBufferGuard.BufferCleaner newBufferCleaner(final MethodHandle unmapper) {
        assert Objects.equals(methodType(void.class, ByteBuffer.class), unmapper.type());
        return (String resourceDescription, ByteBuffer buffer) -> {
            if (!buffer.isDirect()) {
                throw new IllegalArgumentException("unmapping only works with direct buffers");
            }
            final Throwable error =
                    doPrivileged(
                            () -> {
                                try {
                                    unmapper.invokeExact(buffer);
                                    return null;
                                } catch (Throwable t) {
                                    return t;
                                }
                            });
            if (error != null) {
                throw new IOException("Unable to unmap the mapped buffer: " + resourceDescription, error);
            }
        };
    }


    private static <T> T doPrivileged(PrivilegedAction<T> action) {
        return AccessController.doPrivileged(action);
    }

}
