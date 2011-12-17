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

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Builders {

    public static <T extends BuilderConvention> T newInstanceOf(Class<T> aInterface) {
        try {
            ClassLoader classLoader = aInterface.getClassLoader();
            Class[] interfaces = {aInterface};
            InnerInvocationHandler handler = new InnerInvocationHandler(aInterface.getMethods());
            return (T) Proxy.newProxyInstance(classLoader, interfaces, handler);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class InnerInvocationHandler implements InvocationHandler {

        private final Method[] optionMethods;
        private final Object[] optionValues;
        private final Object[] optionDefaultValues;
        private final Method buildMethod;

        public InnerInvocationHandler(Method[] proxyMethods) throws Exception {
            int length = proxyMethods.length - 1; // exclude method "build"
            this.optionValues = new Object[length];
            this.optionDefaultValues = new Object[length];
            this.optionMethods = new Method[length];
            this.buildMethod = filterAndSetDefaultValueAndGetBuildMethod(proxyMethods);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(buildMethod)) return build();

            validate(args[0], method);

            set(method, args[0]);
            return proxy;
        }

        private void validate(Object value, Method method) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotation instanceof DefaultValue) continue;
                if (annotation instanceof ArgumentIndex) continue;
                Validators.validator(annotation).validate(method.getName(), value);
            }
        }

        private Object getDefaultValueBy(String value, Class<?> aClass) throws Exception {
            if (aClass.equals(String.class)) return value;
            return Reflections.cast(value, aClass);
        }

        public Object build() {
            checkOrSetDefaultValue();
            try {
                return Reflections.newInstanceOf(buildMethod.getReturnType(), optionValues);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void checkOrSetDefaultValue() {
            for (int i = 0; i < optionValues.length; i++) {
                if (optionValues[i] != null) continue;
                if (optionDefaultValues[i] == null) throw new NullPointerException(optionMethods[i].getName());
                optionValues[i] = optionDefaultValues[i];
            }
        }

        private Method filterAndSetDefaultValueAndGetBuildMethod(Method[] proxyMethods) throws Exception {
            // TODO refactor this method
            Method buildMethod = null;
            for (Method proxyMethod : proxyMethods) {
                if (proxyMethod.getName().equals("build")) {
                    buildMethod = proxyMethod;
                    continue;
                }

                ArgumentIndex argumentIndex = proxyMethod.getAnnotation(ArgumentIndex.class);

                DefaultValue defaultValue = proxyMethod.getAnnotation(DefaultValue.class);
                if (defaultValue != null) {
                    Class<?> aClass = proxyMethod.getParameterTypes()[0];
                    optionDefaultValues[argumentIndex.value()] = getDefaultValueBy(defaultValue.value(), aClass);
                }
                optionMethods[argumentIndex.value()] = proxyMethod;
            }
            return buildMethod;
        }

        public void set(Method method, Object value) {
            for (int i = 0; i < optionMethods.length; i++) {
                if (!optionMethods[i].equals(method)) continue;
                checkState(optionValues[i] == null, "Can't repeat set " + method);
                optionValues[i] = value;
                return;
            }
        }

    }
}
