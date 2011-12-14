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
import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Validators {

    private static final Validator NULL_VALIDATOR = new Validator() {
        @Override
        public void validate(Object arg) throws IllegalArgumentException { }
    };

    public static Validator validator(Annotation annotation) {
        try {
            Method method = Validators.class.getDeclaredMethod("validator", annotation.annotationType());
            return (Validator) method.invoke(null, annotation);
        } catch (ClassCastException e) {
            return NULL_VALIDATOR;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Validator validator(GreaterThan annotation) {
        final String value = annotation.value();
        return new Validator() {
            @Override
            public void validate(Object arg) throws IllegalArgumentException {
                Comparable expect = comparableNumber(arg, value);
                checkArgument(expect.compareTo(arg) < 0, "Value should be greater than " + expect);
            }
        };
    }

    private static Validator validator(GreaterThanOrEqual annotation) {
        final String value = annotation.value();
        return new Validator() {
            @Override
            public void validate(Object arg) throws IllegalArgumentException {
                Comparable expect = comparableNumber(arg, value);
                checkArgument(expect.compareTo(arg) <= 0, "Value should be greater than or equal " + expect);
            }
        };
    }

    private static Validator validator(LessThan annotation) {
        final String value = annotation.value();
        return new Validator() {
            @Override
            public void validate(Object arg) throws IllegalArgumentException {
                Comparable expect = comparableNumber(arg, value);
                checkArgument(expect.compareTo(arg) > 0, "Value should be less than " + expect);
            }
        };
    }

    private static Validator validator(LessThanOrEqual annotation) {
        final String value = annotation.value();
        return new Validator() {
            @Override
            public void validate(Object arg) throws IllegalArgumentException {
                Comparable expect = comparableNumber(arg, value);
                checkArgument(expect.compareTo(arg) >= 0, "Value should be less than or equal " + expect);
            }
        };
    }

    private static Validator validator(Match annotation) {
        final String value = annotation.value();
        return new Validator() {
            @Override
            public void validate(Object arg) throws IllegalArgumentException {
                checkArgument(((String) arg).matches(value), "Value should match " + value);
            }
        };
    }

    private static Validator validator(NotNull annotation) {
        return new Validator() {
            @Override
            public void validate(Object arg) throws IllegalArgumentException {
                checkNotNull(arg, "Value should not be null");
            }
        };
    }

    private static Comparable comparableNumber(Object arg, String value) {
        try {
            return (Comparable) arg.getClass().getMethod("valueOf", String.class).invoke(arg, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
