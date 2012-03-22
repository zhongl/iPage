/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ephemeron;

import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FlowControllerTest {

    private FlowController controller;

    @Before
    public void setUp() throws Exception {
        controller = new FlowController(1, 1L, NANOSECONDS);
    }

    @Test
    public void overflow() throws Exception {
        assertThat(controller.tryAcquire(), is(true));
        assertThat(controller.tryAcquire(), is(false));
    }

    @Test
    public void release() throws Exception {
        assertThat(controller.tryAcquire(), is(true));
        assertThat(controller.tryAcquire(), is(false));
        controller.release();
        assertThat(controller.tryAcquire(), is(true));
    }

    @Test
    public void adjustThroughout() throws Exception {
        assertThat(controller.throughout(0), is(1));
        assertThat(controller.throughout(1), is(2));
        assertThat(controller.throughout(-1), is(1));
    }
}
