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

package com.github.zhongl.ex.util;

import com.github.zhongl.engine.CallByCountOrElapse;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CallByCountOrElapseTest {

    private int count = 3;
    private long elapseMilliseconds = 100L;
    private com.github.zhongl.engine.CallByCountOrElapse callByCountOrElapse;

    @Before
    public void setUp() throws Exception {
        callByCountOrElapse = new CallByCountOrElapse(count, elapseMilliseconds, new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                return null;
            }
        });
    }

    @Test
    public void runByElapseFirst() throws Exception {
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
        Thread.sleep(elapseMilliseconds);
        assertThat(callByCountOrElapse.tryCallByElapse(), is(true)); // run and reset
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
    }

    @Test
    public void runByCountFirst() throws Exception {
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
        Thread.sleep(elapseMilliseconds / 2);
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
        assertThat(callByCountOrElapse.tryCallByCount(), is(true));  // run and reset
        Thread.sleep(elapseMilliseconds / 2);
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
    }
}
