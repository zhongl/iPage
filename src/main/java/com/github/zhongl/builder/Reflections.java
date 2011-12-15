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

package com.github.zhongl.builder;

import java.lang.reflect.Constructor;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Reflections {
    private Reflections() {}

    public static Object cast(String value, Class aClass) throws Exception {
        return getBoxClassOrReturn(aClass).getMethod("valueOf", String.class).invoke(null, value);
    }

    public static Object newInstanceOf(Class<?> aClass, final Object[] args) throws Exception {
        Constructor<?>[] constructors = aClass.getDeclaredConstructors();
        for (final Constructor<?> constructor : constructors) {
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            if (parameterTypes.length != args.length) continue;
            if (typeNotMatch(parameterTypes, args)) continue;
            constructor.setAccessible(true);
            return constructor.newInstance(args);
        }
        throw new NoSuchMethodException();
    }

    private static Class getBoxClassOrReturn(Class aClass) {
        if (aClass.equals(boolean.class)) return Boolean.class;
        if (aClass.equals(int.class)) return Integer.class;
        if (aClass.equals(float.class)) return Float.class;
        if (aClass.equals(long.class)) return Long.class;
        if (aClass.equals(double.class)) return Double.class;
        return aClass;
    }

    private static boolean typeNotMatch(Class<?>[] parameterTypes, Object[] args) {
        for (int i = 0; i < parameterTypes.length; i++) {
            if (parameterTypes[i].equals(Object.class)) continue;
            if (!getBoxClassOrReturn(parameterTypes[i]).isAssignableFrom(args[i].getClass())) return true;
        }
        return false;
    }

}
