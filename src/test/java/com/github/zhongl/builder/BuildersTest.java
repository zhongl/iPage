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

import org.junit.Test;

import static com.github.zhongl.builder.Builders.newInstanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BuildersTest {

    public static class A {
        final int intValue;
        final long longValue;
        final float floatValue;
        final double doubleValue;
        final Object objectValue;
        final String ipAddress;

        public A(int intValue,
                 long longValue,
                 float floatValue,
                 double doubleValue,
                 Object objectValue,
                 String ipAddress) {
            this.intValue = intValue;
            this.longValue = longValue;
            this.floatValue = floatValue;
            this.doubleValue = doubleValue;
            this.objectValue = objectValue;
            this.ipAddress = ipAddress;
        }

    }

    public interface Builder extends BuilderConvention {

        @ArgumentIndex(0)
        @GreaterThan("0")
        @DefaultValue("7")
        Builder intValue(int value);

        @ArgumentIndex(1)
        @GreaterThanOrEqual("0")
        Builder longValue(long value);

        @ArgumentIndex(2)
        @LessThan("7")
        Builder floatValue(float value);

        @ArgumentIndex(3)
        @LessThanOrEqual("7")
        Builder doubleValue(double value);

        @ArgumentIndex(4)
        @NotNull
        Builder objectValue(Object value);

        @ArgumentIndex(5)
        @Match("(\\d{1,3}\\.){3}\\d{1,3}")
        Builder ipAddress(String value);

        A build();
    }

    @Test
    public void greaterThanOK() throws Exception {
        newInstanceOf(Builder.class).intValue(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void greaterThanError() throws Exception {
        newInstanceOf(Builder.class).intValue(0);
    }

    @Test
    public void lessThanOK() throws Exception {
        newInstanceOf(Builder.class).floatValue(6.9f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void lessThanError() throws Exception {
        newInstanceOf(Builder.class).floatValue(7.1f);
    }

    @Test
    public void greaterThanOrEqualOK() throws Exception {
        newInstanceOf(Builder.class).longValue(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void greaterThanOrEqualError() throws Exception {
        newInstanceOf(Builder.class).longValue(-1);
    }

    @Test
    public void lessThanOrEqualOK() throws Exception {
        newInstanceOf(Builder.class).doubleValue(7.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void lessThanOrEqualError() throws Exception {
        newInstanceOf(Builder.class).doubleValue(7.01);
    }

    @Test
    public void notNullOk() throws Exception {
        newInstanceOf(Builder.class).objectValue("");
    }

    @Test(expected = NullPointerException.class)
    public void notNullError() throws Exception {
        newInstanceOf(Builder.class).objectValue(null);
    }

    @Test
    public void matchOk() throws Exception {
        newInstanceOf(Builder.class).ipAddress("127.0.0.1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void matchError() throws Exception {
        newInstanceOf(Builder.class).ipAddress("null");
    }

    @Test(expected = IllegalStateException.class)
    public void setTwiceError() throws Exception {
        Builder builder = newInstanceOf(Builder.class);
        builder.objectValue("");
        builder.objectValue("");
    }

    @Test
    public void defaultIntValue() throws Exception {
        Builder builder = newInstanceOf(Builder.class);
        A a = builder.longValue(0L)
            .floatValue(6.9f)
            .doubleValue(7.0)
            .objectValue("")
            .ipAddress("127.0.0.1")
            .build();

        assertThat(a.intValue, is(7));
        assertThat(a.longValue, is(0L));
        assertThat(a.floatValue, is(6.9f));
        assertThat(a.doubleValue, is(7.0));
        assertThat((String) a.objectValue, is(""));
        assertThat(a.ipAddress, is("127.0.0.1"));
    }

    @Test
    public void setIntValue() throws Exception {
        Builder builder = newInstanceOf(Builder.class);
        A a = builder.intValue(8)
            .longValue(0L)
            .floatValue(6.9f)
            .doubleValue(7.0)
            .objectValue("")
            .ipAddress("127.0.0.1")
            .build();

        assertThat(a.intValue, is(8));
        assertThat(a.longValue, is(0L));
        assertThat(a.floatValue, is(6.9f));
        assertThat(a.doubleValue, is(7.0));
        assertThat((String) a.objectValue, is(""));
        assertThat(a.ipAddress, is("127.0.0.1"));
    }

    @Test(expected = NullPointerException.class)
    public void notSetLongValue() throws Exception {
        Builder builder = newInstanceOf(Builder.class);
        A a = builder.floatValue(6.9f)
            .doubleValue(7.0)
            .objectValue("")
            .ipAddress("127.0.0.1")
            .build();

        assertThat(a.intValue, is(8));
        assertThat(a.longValue, is(0L));
        assertThat(a.floatValue, is(6.9f));
        assertThat(a.doubleValue, is(7.0));
        assertThat((String) a.objectValue, is(""));
        assertThat(a.ipAddress, is("127.0.0.1"));
    }
}
