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

package com.trs.pacifica.core;

import com.trs.pacifica.LogStorage;
import com.trs.pacifica.PacificaServiceFactory;
import com.trs.pacifica.SnapshotStorage;
import com.trs.pacifica.error.PacificaErrorCode;
import com.trs.pacifica.error.PacificaException;
import com.trs.pacifica.log.FsLogStorage;
import com.trs.pacifica.snapshot.storage.DefaultSnapshotStorage;
import com.trs.pacifica.spi.SPI;

import java.io.IOException;

@SPI
public class DefaultPacificaServiceFactory implements PacificaServiceFactory {
    @Override
    public LogStorage newLogStorage(final String path) throws PacificaException {
        try {
            return new FsLogStorage(path, null, null, null);
        } catch (IOException e) {
            throw new PacificaException(PacificaErrorCode.IO, String.format("failed to new LogStorage, error_msg=%s", e.getMessage()), e);
        }
    }

    @Override
    public SnapshotStorage newSnapshotStorage(final String path) throws PacificaException {
        try {
            return new DefaultSnapshotStorage(path);
        } catch (IOException e) {
            throw new PacificaException(PacificaErrorCode.IO, String.format("failed to new SnapshotStorage, error_msg=%s", e.getMessage()), e);
        }
    }
}
