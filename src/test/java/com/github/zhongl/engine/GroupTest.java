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

package com.github.zhongl.engine;

import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class GroupTest {

    private Group group;
    private AssertFutureCallback callback0;
    private AssertFutureCallback callback1;

    @Before
    public void setUp() throws Exception {
        group = Group.newInstance();
        callback0 = new AssertFutureCallback();
        callback1 = new AssertFutureCallback();
        FutureCallback<Object> decorate0 = group.decorate(callback0);
        FutureCallback<Object> decorate1 = group.decorate(callback1);

        group.register(decorate0);
        group.register(decorate1);
    }

    @Test
    public void commit() throws Exception {
        callback0.assertNotDone();
        callback1.assertNotDone();
        group.commit();
        callback0.assertDone();
        callback1.assertDone();
    }

    @Test
    public void rollback() throws Exception {
        callback0.assertNotDone();
        callback1.assertNotDone();
        Exception e = new Exception();
        group.rollback(e);
        callback0.assertFailure(e);
        callback1.assertFailure(e);
    }

    @Test
    public void onFailure() throws Exception {
        IOException e = new IOException();
        group.decorate(callback0).onFailure(e);
        callback0.assertFailure(e);
    }

    private static class AssertFutureCallback implements FutureCallback<Object> {
        private Throwable t;
        private boolean done = false;

        @Override
        public void onSuccess(Object result) {
            done = true;
        }

        @Override
        public void onFailure(Throwable t) {
            this.t = t;
        }

        public void assertDone() {
            assertThat(done, is(true));
        }

        public void assertNotDone() {
            assertThat(done, is(false));
        }

        public void assertFailure(Throwable t) {
            assertThat(this.t, is(t));
        }
    }
}
