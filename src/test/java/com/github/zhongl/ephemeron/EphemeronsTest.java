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

import com.github.zhongl.api.Md5Key;
import com.github.zhongl.index.Key;
import com.github.zhongl.util.FutureCallbacks;
import com.github.zhongl.util.Md5;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class EphemeronsTest {

    private FutureCallback<Void> ignore;
    private Storage storage;
    private Ephemerons<Integer> ephemerons;

    @Before
    public void setUp() throws Exception {
        ignore = FutureCallbacks.ignore();
        storage = mock(Storage.class);
        ephemerons = new Ephemerons<Integer>(storage, new FlowController(1, 1L, NANOSECONDS));
    }

    @Test
    public void addAndRemoveImmediately() throws Exception {
        int one = 1;
        Key key = key(one);

        ephemerons.add(key, one, ignore);
        assertThat(ephemerons.get(key), is(one));

        ephemerons.remove(key, ignore);
        assertThat(ephemerons.get(key), is(nullValue()));
    }

    @Test
    public void getFromStorage() throws Exception {
        int one = 1;
        Key key = key(one);

        ephemerons.add(key, one, ignore);
        ephemerons.flush();

        Modification modification = null;
        verify(storage).merge(modification);
        assertThat(ephemerons.get(key), is(nullValue()));

        doReturn(one).when(storage).get(key);
        assertThat(ephemerons.get(key), is(one));
    }

    private Key key(int i) {
        return new Md5Key(Md5.md5(Ints.toByteArray(i)));
    }
}
