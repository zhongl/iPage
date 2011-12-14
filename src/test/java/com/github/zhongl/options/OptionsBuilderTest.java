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

import org.junit.Test;

import static com.github.zhongl.options.OptionsBuilder.newInstanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class OptionsBuilderTest {

    public interface TestOptions extends Options {
        @GreaterThan("0")
        TestOptions intValue(int value);

        @GreaterThanOrEqual("0")
        TestOptions longValue(long value);

        @LessThan("7")
        TestOptions floatValue(float value);

        @LessThanOrEqual("7")
        TestOptions doubleValue(double value);

        @NotNull
        TestOptions objectValue(Object value);

        @Match("(\\d{1,3}\\.){3}\\d{1,3}")
        TestOptions ipAddress(String value);
    }

    @Test
    public void get() throws Exception {
        TestOptions options = newInstanceOf(TestOptions.class);
        options.intValue(4);
        assertThat(options.get(Integer.class, "intValue"), is(4));
    }

    @Test(expected = NullPointerException.class)
    public void getNull() throws Exception {
        TestOptions options = newInstanceOf(TestOptions.class);
        assertThat(options.get(Integer.class, "intValue"), is(4));
    }

    @Test
    public void getOrDefault() throws Exception {
        TestOptions options = newInstanceOf(TestOptions.class);
        assertThat(options.getOrDefault(Integer.class, "intValue", 7), is(7));
    }

    @Test
    public void greaterThanOK() throws Exception {
        newInstanceOf(TestOptions.class).intValue(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void greaterThanError() throws Exception {
        newInstanceOf(TestOptions.class).intValue(0);
    }

    @Test
    public void lessThanOK() throws Exception {
        newInstanceOf(TestOptions.class).floatValue(6.9f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void lessThanError() throws Exception {
        newInstanceOf(TestOptions.class).floatValue(7.1f);
    }

    @Test
    public void greaterThanOrEqualOK() throws Exception {
        newInstanceOf(TestOptions.class).longValue(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void greaterThanOrEqualError() throws Exception {
        newInstanceOf(TestOptions.class).longValue(-1);
    }

    @Test
    public void lessThanOrEqualOK() throws Exception {
        newInstanceOf(TestOptions.class).doubleValue(7.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void lessThanOrEqualError() throws Exception {
        newInstanceOf(TestOptions.class).doubleValue(7.01);
    }

    @Test
    public void notNullOk() throws Exception {
        newInstanceOf(TestOptions.class).objectValue("");
    }

    @Test(expected = NullPointerException.class)
    public void notNullError() throws Exception {
        newInstanceOf(TestOptions.class).objectValue(null);
    }

    @Test
    public void matchOk() throws Exception {
        newInstanceOf(TestOptions.class).ipAddress("127.0.0.1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void matchError() throws Exception {
        newInstanceOf(TestOptions.class).ipAddress("null");
    }

    @Test(expected = IllegalStateException.class)
    public void setTwiceError() throws Exception {
        TestOptions options = newInstanceOf(TestOptions.class);
        options.objectValue("");
        options.objectValue("");
    }
}
