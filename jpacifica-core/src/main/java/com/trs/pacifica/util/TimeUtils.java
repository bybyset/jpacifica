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

package com.trs.pacifica.util;

import java.util.concurrent.TimeUnit;

public class TimeUtils {


    private TimeUtils() {

    }

    /**
     * Gets the current monotonic time in milliseconds.
     *
     * @return monotonicMs
     */
    public static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    /**
     * Returns the current time in milliseconds, it's not monotonic, would be forwarded/backward by
     * clock synchronous.
     *
     * @return currentTimeMillis
     */
    public static long nowMs() {
        return System.currentTimeMillis();
    }

    /**
     * Gets the current monotonic time in microseconds.
     *
     * @return monotonicUs
     */
    public static long monotonicUs() {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    }

    public static void main(String[] args) {
        System.out.printf(monotonicMs() + " ");
    }


}
