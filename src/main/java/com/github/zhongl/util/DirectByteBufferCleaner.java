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

    public static void clean(final ByteBuffer byteBuffer) {
        if (!byteBuffer.isDirect()) return;
        try {
            Object cleaner = invoke(byteBuffer, "cleaner");
            invoke(cleaner, "clean");
        } catch (Exception e) { /* ignore */ }
    }

    private static Object invoke(final Object target, String methodName) throws Exception {
        final Method method = target.getClass().getMethod(methodName);
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

}
