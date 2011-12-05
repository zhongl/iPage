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

package com.github.zhongl.ipage;

import com.github.zhongl.util.DirBase;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineTest extends DirBase {
    private KVEngine engine;

    @After
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.shutdown();
            engine.awaitForShutdown(Integer.MAX_VALUE);
        }
    }

    @Test
    public void putAndGetAndRemove() throws Exception {
        dir = testDir("putAndGetAndRemove");
        engine = KVEngine.baseOn(dir).build();
        engine.startup();

        Record record = new Record("record".getBytes());
        Md5Key key = Md5Key.valueOf(record);

        assertThat(engine.put(key, record), is(nullValue()));

        assertThat(engine.get(key), is(record));
        assertThat(engine.remove(key), is(record));
    }

    @Test
    public void getAndRemoveNonExistKey() throws Exception {
        dir = testDir("getAndRemoveNonExistKey");
        engine = KVEngine.baseOn(dir).build();
        engine.startup();
        Md5Key key = Md5Key.valueOf("non-exist".getBytes());
        assertThat(engine.get(key), is(nullValue()));
        assertThat(engine.remove(key), is(nullValue()));
    }

    @Test
    public void flushByCountFirst() throws Exception {
        dir = testDir("flushByCountFirst");
        dir.mkdirs();

        IPage ipage = mock(IPage.class);
        Index index = mock(Index.class);
        int count = 2;
        long elapseMilliseconds = 100L;
        Callable<?> flusher = mock(Callable.class);

        DataSecurity dataSecurity = new DataSecurity(dir);
        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(count, elapseMilliseconds, flusher);
        engine = new KVEngine(10L, 10, Group.NULL, ipage, index, callByCountOrElapse, dataSecurity);
        engine.startup();

        engine.put(mock(Md5Key.class), mock(Record.class));
        Thread.sleep(elapseMilliseconds / 2);
        engine.put(mock(Md5Key.class), mock(Record.class)); // flush by count and reset
        Thread.sleep(elapseMilliseconds / 2);

        verify(flusher, times(1)).call();
    }

    @Test
    public void flushByElapseFirst() throws Exception {
        dir = testDir("flushByElapseFirst");
        dir.mkdirs();

        IPage ipage = mock(IPage.class);
        Index index = mock(Index.class);
        int count = 2;
        long elapseMilliseconds = 100L;
        Callable<?> flusher = mock(Callable.class);

        DataSecurity dataSecurity = new DataSecurity(dir);
        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(count, elapseMilliseconds, flusher);
        engine = new KVEngine(10L, 10, Group.NULL, ipage, index, callByCountOrElapse, dataSecurity);
        engine.startup();

        engine.put(mock(Md5Key.class), mock(Record.class));
        Thread.sleep(elapseMilliseconds);                   // flush by elapse and reset
        engine.put(mock(Md5Key.class), mock(Record.class));

        verify(flusher, times(1)).call();
    }

}