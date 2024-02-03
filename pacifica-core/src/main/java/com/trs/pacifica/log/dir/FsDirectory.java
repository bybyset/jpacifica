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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public abstract class FsDirectory extends BaseDirectory{

    protected final Path directory;

    private final AtomicLong fileCounter = new AtomicLong(0);

    public FsDirectory(Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            Files.createDirectories(path); // create directory, if it doesn't exist
        }
        directory = path.toRealPath();
    }


    @Override
    public String[] listAll() throws IOException {
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

    }

    @Override
    public DataInOutput createDataInOutput(String name) throws IOException {
        return null;
    }
}
