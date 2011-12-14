/*
 * Copyright 2011 zhongl
 *
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

package com.github.zhongl.options;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class OptionsBuilder {
    private static final Method OPTIONS_GET;
    private static final Method OPTIONS_GET_OR_DEFAULT;

    static {
        try {
            OPTIONS_GET = Options.class.getMethod("get", Class.class, String.class);
            OPTIONS_GET_OR_DEFAULT = Options.class.getMethod("getOrDefault", Class.class, String.class, Object.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T extends Options> T newInstanceOf(Class<T> aInterface) {
        ClassLoader classLoader = aInterface.getClassLoader();
        Class<T>[] interfaces = new Class[] {aInterface};
        InvocationHandler handler = new InnerInvocationHandler();
        return (T) Proxy.newProxyInstance(classLoader, interfaces, handler);
    }

    private static class InnerInvocationHandler implements InvocationHandler {

        private final Map<String, Object> map = new HashMap<String, Object>();

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(OPTIONS_GET)) return get(((Class) args[0]), (String) args[1]);

            if (method.equals(OPTIONS_GET_OR_DEFAULT))
                return getOrDefault(((Class) args[0]), (String) args[1], args[2]);

            for (Annotation annotation : method.getAnnotations()) {
                Validators.validator(annotation).validate(args[0]);
            }

            set(method.getName(), args[0]);
            return proxy;
        }

        private Object getOrDefault(Class clazz, String name, Object defaultValue) {
            try {
                return get(clazz, name);
            } catch (NullPointerException e) {
                return defaultValue;
            }
        }

        private Object get(Class clazz, String name) {
            Object value = map.get(name);
            if (value == null) throw new NullPointerException(name + " should not be null");
            return clazz.cast(value);
        }

        private void set(String name, Object value) {
            checkState(map.put(name, value) == null, "Can't repeat set " + name);
        }

    }
}
