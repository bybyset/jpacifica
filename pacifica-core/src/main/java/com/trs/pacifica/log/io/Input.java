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

import java.io.IOException;

public interface Input {

    /**
     * Reads and returns a single byte.
     */
    public byte readByte() throws IOException;

    /**
     * Sets current position in this file, where the next read will occur. If this is beyond the end
     * of the file then this will throw {@code EOFException} and then the stream is in an undetermined
     * state.
     */
    public abstract void seek(long pos) throws IOException;


}
