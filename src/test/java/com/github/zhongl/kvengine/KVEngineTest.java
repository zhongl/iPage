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

package com.github.zhongl.kvengine;

import com.github.zhongl.accessor.CommonAccessors;
import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.ipage.IPage;
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineTest extends FileBase {

    protected BlockingKVEngine<String> engine;

    @Test
    public void putAndGetAndRemove() throws Exception {
        dir = testDir("putAndGetAndRemove");
        newEngineAndStartup();

        String value = "value";
        Md5Key key = Md5Key.generate(value.getBytes());

        assertThat(engine.put(key, value), is(nullValue()));
        assertThat(engine.get(key), is(value));
        assertThat(engine.remove(key), is(value));
    }

    private void newEngineAndStartup() throws IOException {
        engine = new BlockingKVEngine<String>(KVEngine.<String>baseOn(dir).valueAccessor(CommonAccessors.STRING).build());
        engine.startup();
    }

    @Test
    public void getAndRemoveNonExistKey() throws Exception {
        dir = testDir("getAndRemoveNonExistKey");
        newEngineAndStartup();
        Md5Key key = Md5Key.generate("non-exist".getBytes());
        assertThat(engine.get(key), is(nullValue()));
        assertThat(engine.remove(key), is(nullValue()));
    }

    @Test
    public void flushByCountFirst() throws Exception {
        dir = testDir("flushByCountFirst");
        dir.mkdirs();

        IPage<Entry<String>> ipage = mock(IPage.class);
        doReturn(new Entry(mock(Md5Key.class), "")).when(ipage).get(anyLong());
        Index index = mock(Index.class);
        int count = 2;
        long elapseMilliseconds = 100L;
        Callable<?> flusher = mock(Callable.class);

        DataIntegerity dataIntegerity = new DataIntegerity(dir);
        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(count, elapseMilliseconds, flusher);

        Operation<String> operation = new Operation<String>(ipage, index, Group.NULL, callByCountOrElapse);
        engine = new BlockingKVEngine<String>(new KVEngine<String>(10L, 10, dataIntegerity, operation));
        engine.startup();

        engine.put(mock(Md5Key.class), "");
        Thread.sleep(elapseMilliseconds / 2);
        engine.put(mock(Md5Key.class), ""); // flush by count and reset
        Thread.sleep(elapseMilliseconds / 2);

        verify(flusher, times(1)).call();
    }

    @Test
    @Ignore("TODO")
    public void flushByElapseFirst() throws Exception {
        dir = testDir("flushByElapseFirst");
        dir.mkdirs();

        IPage<Entry<String>> ipage = mock(IPage.class);
        doReturn(new Entry(mock(Md5Key.class), "")).when(ipage).get(anyLong());

        Index index = mock(Index.class);
        int count = 2;
        long elapseMilliseconds = 100L;
        Callable<?> flusher = mock(Callable.class);

        DataIntegerity dataIntegerity = new DataIntegerity(dir);
        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(count, elapseMilliseconds, flusher);

        Operation<String> operation = new Operation<String>(ipage, index, Group.NULL, callByCountOrElapse);
        engine = new BlockingKVEngine<String>(new KVEngine<String>(10L, 10, dataIntegerity, operation));
        engine.startup();

        engine.put(mock(Md5Key.class), "");
        Thread.sleep(elapseMilliseconds);                   // flush by elapse and reset
        engine.put(mock(Md5Key.class), "");

        verify(flusher, times(1)).call();
    }

    @Test
    @Ignore("TODO")
    public void iterator() throws Exception {
        dir = testDir("valueIterator");

        newEngineAndStartup();

        String value0 = "value0";
        engine.put(Md5Key.generate(value0.getBytes()), value0);
        String value1 = "value1";
        engine.put(Md5Key.generate(value1.getBytes()), value1);
        String value2 = "value2";
        engine.put(Md5Key.generate(value2.getBytes()), value2);

        engine.remove(Md5Key.generate(value1.getBytes()));

        Iterator<String> iterator = engine.valueIterator();

        assertThat(iterator.next(), is(value0));
        assertThat(iterator.next(), is(value2));
        assertThat(iterator.hasNext(), is(false));

    }

    @After
    public void tearDown() throws Exception {
        if (engine != null) engine.shutdown();
    }
}
