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

    public static class TestObject {
        final int intValue;
        final long longValue;
        final float floatValue;
        final double doubleValue;
        final Object objectValue;
        final String ipAddress;

        public TestObject(int intValue,
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

        @GreaterThan("0")
        @DefaultValue("7")
        Builder intValue(int value);

        @GreaterThanOrEqual("0")
        Builder longValue(long value);

        @LessThan("7")
        Builder floatValue(float value);

        @LessThanOrEqual("7")
        Builder doubleValue(double value);

        @NotNull
        Builder objectValue(Object value);

        @Match("(\\d{1,3}\\.){3}\\d{1,3}")
        Builder ipAddress(String value);

        TestObject build();
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
    public void build() throws Exception {
        Builder builder = newInstanceOf(Builder.class);
        TestObject testObject = builder.longValue(0L)
                                       .floatValue(6.9f)
                                       .doubleValue(7.0)
                                       .objectValue("")
                                       .ipAddress("127.0.0.1")
                                       .build();

        assertThat(testObject.intValue, is(7));
        assertThat(testObject.longValue, is(0L));
        assertThat(testObject.floatValue, is(6.9f));
        assertThat(testObject.doubleValue, is(7.0));
        assertThat((String) testObject.objectValue, is(""));
        assertThat(testObject.ipAddress, is("127.0.0.1"));
    }
}
