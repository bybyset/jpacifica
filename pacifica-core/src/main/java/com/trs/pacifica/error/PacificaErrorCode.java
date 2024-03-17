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

package com.trs.pacifica.error;

import java.util.HashMap;
import java.util.Map;

public enum PacificaErrorCode {


    UNDEFINED(999999, "undefined"),

    CONFLICT_LOG(100001, "conflict log")
    ;


    private final int code;

    private final String desc;


    PacificaErrorCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return "PacificaError{" +
                "code=" + code +
                ", desc='" + desc + '\'' +
                '}';
    }


    private static final Map<Integer, PacificaErrorCode> CODE_ERROR_MAP = new HashMap<>();

    static {
        for (final PacificaErrorCode error : PacificaErrorCode.values()) {
            CODE_ERROR_MAP.putIfAbsent(error.code, error);
        }
    }

    public static PacificaErrorCode fromCode(final int code) {
        return CODE_ERROR_MAP.getOrDefault(code, UNDEFINED);
    }

}
