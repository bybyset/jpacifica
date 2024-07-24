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

package com.trs.pacifica.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LogIdTest {


    @Test
    public void testCompare() {
        LogId logId1 = new LogId(1003, 1);
        LogId logId2 = new LogId(1003, 1);
        LogId logId3 = new LogId(1003, 2);
        LogId logId4 = new LogId(1004, 2);

        Assertions.assertEquals(logId1, logId2);
        Assertions.assertTrue(logId3.compareTo(logId2) > 0);
        Assertions.assertTrue(logId1.compareTo(logId3) < 0);
        Assertions.assertTrue(logId4.compareTo(logId3) > 0);

    }

    @Test
    public void testCopy() {
        LogId logId1 = new LogId(1003, 1);
        LogId logId2 = logId1.copy();

        Assertions.assertEquals(logId1, logId2);
        Assertions.assertFalse(logId1 == logId2);

    }

}
