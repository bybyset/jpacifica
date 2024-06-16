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

package com.trs.pacifica.log.store.file;

import com.trs.pacifica.model.LogId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

public class IndexEntryTest {

    IndexEntry.IndexEntryCodec indexEntryCodec = new IndexEntry.IndexEntryCodecV1();

    @Test
    public void testIndexEntryCodec() {
        IndexEntry indexEntry = new IndexEntry(new LogId(1001, 1), 20);
        final byte[] encode = indexEntryCodec.encode(indexEntry);
        IndexEntry decode = indexEntryCodec.decode(encode);
        Assertions.assertEquals(indexEntry, decode);
    }

}