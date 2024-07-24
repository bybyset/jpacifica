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

import com.google.common.collect.Lists;

import java.util.List;
import java.util.function.BiFunction;

public class ObjectsUtil {


    /**
     * Maps out-of-bounds values to a runtime exception.
     *
     * @param checkKind the kind of bounds check, whose name may correspond
     *                  to the name of one of the range check methods, checkIndex,
     *                  checkFromToIndex, checkFromIndexSize
     * @param args      the out-of-bounds arguments that failed the range check.
     *                  If the checkKind corresponds a the name of a range check method
     *                  then the bounds arguments are those that can be passed in order
     *                  to the method.
     * @param oobef     the exception formatter that when applied with a checkKind
     *                  and a list out-of-bounds arguments returns a runtime exception.
     *                  If {@code null} then, it is as if an exception formatter was
     *                  supplied that returns {@link IndexOutOfBoundsException} for any
     *                  given arguments.
     * @return the runtime exception
     */
    private static RuntimeException outOfBounds(
            BiFunction<String, List<Number>, ? extends RuntimeException> oobef,
            String checkKind,
            Number... args) {
        List<Number> largs = Lists.newArrayList(args);
        RuntimeException e = oobef == null
                ? null : oobef.apply(checkKind, largs);
        return e == null
                ? new IndexOutOfBoundsException(outOfBoundsMessage(checkKind, largs)) : e;
    }

    private static String outOfBoundsMessage(String checkKind, List<? extends Number> args) {
        if (checkKind == null && args == null) {
            return String.format("Range check failed");
        } else if (checkKind == null) {
            return String.format("Range check failed: %s", args);
        } else if (args == null) {
            return String.format("Range check failed: %s", checkKind);
        }

        int argSize = 0;
        switch (checkKind) {
            case "checkIndex":
                argSize = 2;
                break;
            case "checkFromToIndex":
            case "checkFromIndexSize":
                argSize = 3;
                break;
            default:
        }

        // Switch to default if fewer or more arguments than required are supplied
        switch ((args.size() != argSize) ? "" : checkKind) {
            case "checkIndex":
                return String.format("Index %s out of bounds for length %s",
                        args.get(0), args.get(1));
            case "checkFromToIndex":
                return String.format("Range [%s, %s) out of bounds for length %s",
                        args.get(0), args.get(1), args.get(2));
            case "checkFromIndexSize":
                return String.format("Range [%s, %<s + %s) out of bounds for length %s",
                        args.get(0), args.get(1), args.get(2));
            default:
                return String.format("Range check failed: %s %s", checkKind, args);
        }
    }

    private static RuntimeException outOfBoundsCheckIndex(
            BiFunction<String, List<Number>, ? extends RuntimeException> oobe,
            int index, int length) {
        return outOfBounds(oobe, "checkIndex", index, length);
    }

    private static RuntimeException outOfBoundsCheckFromToIndex(
            BiFunction<String, List<Number>, ? extends RuntimeException> oobe,
            int fromIndex, int toIndex, int length) {
        return outOfBounds(oobe, "checkFromToIndex", fromIndex, toIndex, length);
    }

    private static RuntimeException outOfBoundsCheckFromIndexSize(
            BiFunction<String, List<Number>, ? extends RuntimeException> oobe,
            int fromIndex, int size, int length) {
        return outOfBounds(oobe, "checkFromIndexSize", fromIndex, size, length);
    }

    private static RuntimeException outOfBoundsCheckIndex(
            BiFunction<String, List<Number>, ? extends RuntimeException> oobe,
            long index, long length) {
        return outOfBounds(oobe, "checkIndex", index, length);
    }

    private static RuntimeException outOfBoundsCheckFromToIndex(
            BiFunction<String, List<Number>, ? extends RuntimeException> oobe,
            long fromIndex, long toIndex, long length) {
        return outOfBounds(oobe, "checkFromToIndex", fromIndex, toIndex, length);
    }

    private static RuntimeException outOfBoundsCheckFromIndexSize(
            BiFunction<String, List<Number>, ? extends RuntimeException> oobe,
            long fromIndex, long size, long length) {
        return outOfBounds(oobe, "checkFromIndexSize", fromIndex, size, length);
    }


    public static <X extends RuntimeException> int checkFromIndexSize(int fromIndex, int size, int length,
                                                                      BiFunction<String, List<Number>, X> oobef) {
        if ((length | fromIndex | size) < 0 || size > length - fromIndex)
            throw outOfBoundsCheckFromIndexSize(oobef, fromIndex, size, length);
        return fromIndex;
    }

    public static <X extends RuntimeException> int checkFromToIndex(int fromIndex, int toIndex, int length,
                                                                    BiFunction<String, List<Number>, X> oobef) {
        if (fromIndex < 0 || fromIndex > toIndex || toIndex > length)
            throw outOfBoundsCheckFromToIndex(oobef, fromIndex, toIndex, length);
        return fromIndex;
    }

    public static int checkFromToIndex(int fromIndex, int toIndex, int length) {
        return checkFromToIndex(fromIndex, toIndex, length, null);
    }

    public static int checkFromIndexSize(int fromIndex, int size, int length) {
        return checkFromIndexSize(fromIndex, size, length, null);
    }
}
