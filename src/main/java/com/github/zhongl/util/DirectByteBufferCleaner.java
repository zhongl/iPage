/*
 * Copyright 2012 zhongl
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * {@link DirectByteBufferCleaner}
 *
 * @author <a href=mailto:zhong.lunfu@gmail.com>zhongl</a>
 * @created 2011-1-14
 */
public final class DirectByteBufferCleaner {
    private DirectByteBufferCleaner() {}

    public static void clean(final ByteBuffer buffer) {
        if (!buffer.isDirect()) return;
        try {
            invoke(invoke(viewed(buffer), "cleaner"), "clean");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean isNotCleaned(ByteBuffer buffer) {
        if (!buffer.isDirect()) return false;
        try {
            Long address = (Long) get(get(invoke(viewed(buffer), "cleaner"), "thunk"), "address");
            return address != 0L;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) throws Exception {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static Object get(final Object target, final String fieldName) throws Exception {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Field field = field(target, fieldName);
                    field.setAccessible(true);
                    return field.get(target);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static Field field(Object target, String fieldName) throws NoSuchFieldException {
        try {
            return target.getClass().getField(fieldName);
        } catch (NoSuchFieldException e) {
            return target.getClass().getDeclaredField(fieldName);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) throws Exception {
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, "viewedBuffer");
        if (viewedBuffer == null) return buffer;
        else return viewed(viewedBuffer);
    }
}
