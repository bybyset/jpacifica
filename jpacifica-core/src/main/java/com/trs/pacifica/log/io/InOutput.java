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

import com.trs.pacifica.error.AlreadyClosedException;

import java.io.IOException;

public interface InOutput extends Input, Output, Cloneable{

    /**
     * Creates a slice of this index input, with the given description, offset, and length. The slice
     * is sought to the beginning.
     */
    InOutput slice(String sliceDescription, long offset, long length)
            throws IOException;


    /**
     * {@inheritDoc}
     *
     * <p><b>Warning:</b> never closes cloned {@code IndexInput}s, it will only call {@link
     * #close()} on the original object.
     *
     * <p>If you access the cloned IndexInput after closing the original object, any <code>readXXX
     * </code> methods will throw {@link AlreadyClosedException}.
     *
     * <p>This method is NOT thread safe, so if the current {@code IndexInput} is being used by one
     * thread while {@code clone} is called by another, disaster could strike.
     */
    InOutput clone();

    /**
     *
     * @throws IOException
     */
    @Override
    void close() throws IOException;


}
