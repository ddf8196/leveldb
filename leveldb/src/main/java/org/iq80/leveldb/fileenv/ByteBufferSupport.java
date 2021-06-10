/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.fileenv;

import com.google.common.base.Throwables;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

final class ByteBufferSupport
{
    private static final Object UNSAFE;
    private static final Method INVOKE_CLEANER;

    private static final boolean IS_PRE_JAVA9_IMPL;
    private static final Method GET_CLEANER;

    static {
        Object unsafe;
        Method invoker;

        boolean isPreJava9;
        Method getCleaner;
        try {
            // Java 9 added an invokeCleaner method to Unsafe to work around
            // module visibility issues for code that used to rely on DirectByteBuffer's cleaner()
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);

            unsafe = theUnsafe.get(null);
            invoker = unsafeClass.getDeclaredMethod("invokeCleaner", ByteBuffer.class);
            isPreJava9 = false;
            getCleaner = null;
        }
        catch (Exception e) {
            // fall back to pre-java 9 compatible behavior
            try {
                Class<?> directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
                Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");

                Method cleanerMethod = directByteBufferClass.getDeclaredMethod("cleaner");
                cleanerMethod.setAccessible(true);

                Method cleanMethod = cleanerClass.getDeclaredMethod("clean");
                cleanMethod.setAccessible(true);

                unsafe = null;
                invoker = cleanMethod;
                isPreJava9 = true;
                getCleaner = cleanerMethod;
            }
            catch (Exception e1) {
                throw new AssertionError(e1);
            }
        }

        UNSAFE = unsafe;
        INVOKE_CLEANER = invoker;
        IS_PRE_JAVA9_IMPL = isPreJava9;
        if (isPreJava9) {
            GET_CLEANER = getCleaner;
        }
        else {
            GET_CLEANER = null;
        }
    }

    private ByteBufferSupport()
    {
    }

    public static void unmap(MappedByteBuffer buffer)
    {
        try {
            if (!IS_PRE_JAVA9_IMPL) {
                INVOKE_CLEANER.invoke(UNSAFE, buffer);
            }
            else {
                Object cleaner = GET_CLEANER.invoke(buffer);
                INVOKE_CLEANER.invoke(cleaner);
            }

        }
        catch (Throwable ignored) {
            throw Throwables.propagate(ignored);
        }
    }
}
