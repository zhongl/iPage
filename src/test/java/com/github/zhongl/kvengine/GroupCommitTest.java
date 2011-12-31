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

import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.ipage.IPage;
import com.github.zhongl.sequence.Cursor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * More infomation about group commit is here, https://github.com/zhongl/iPage/issues/2
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class GroupCommitTest {
    private BlockingKVEngine<String> engine;
    private IPage<Entry<String>> ipage;
    private Index index;
    private Callable flusher;
    private Group group;
    private DataIntegrity dataIntegrity;
    private CallByCountOrElapse callByCountOrElapse;

    @After
    public void tearDown() throws Exception {
        if (engine != null) engine.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        ipage = mock(IPage.class);
        doReturn(new Entry(mock(Md5Key.class), "")).when(ipage).get(anyLong());

        index = mock(Index.class);
        flusher = mock(Callable.class);
        group = Group.newInstance();
        dataIntegrity = mock(DataIntegrity.class);
    }

    @Test
    public void groupCommitByCount() throws Exception {
        callByCountOrElapse = new CallByCountOrElapse(2, Long.MAX_VALUE, flusher);
        newEngineAndStartup();

        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<String> putFuture = service.submit(new Put("value"));
        Future<String> removeFuture = service.submit(new Remove("value"));

        putFuture.get();
        removeFuture.get();

        service.shutdown();
    }

    @Test
    public void groupRollbackByCount() throws Exception {
        IOException e = new IOException();
        doThrow(e).when(flusher).call();

        callByCountOrElapse = new CallByCountOrElapse(2, Long.MAX_VALUE, flusher);
        newEngineAndStartup();

        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<String> putFuture = service.submit(new Put("value"));
        Future<String> removeFuture = service.submit(new Remove("value"));

        try {
            putFuture.get();
            fail("No exception.");
        } catch (Throwable t) {
            assertThat(t.getCause(), is((Throwable) e));
        }
        try {
            removeFuture.get();
            fail("No exception.");
        } catch (Throwable t) {
            assertThat(t.getCause(), is((Throwable) e));
        }

        service.shutdown();
    }

    private void newEngineAndStartup() {
        Operation<String> operation = new Operation<String>(ipage, index, group, callByCountOrElapse);
        engine = new BlockingKVEngine<String>(new KVEngine<String>(10L, 10, dataIntegrity, operation));
        engine.startup();
    }

    @Test
    public void groupCommitByElapse() throws Exception {
        callByCountOrElapse = new CallByCountOrElapse(Integer.MAX_VALUE, 10L, flusher);
        newEngineAndStartup();

        String value = "value";
        engine.put(Md5Key.generate(value.getBytes()), value);
    }

    @Test(expected = IOException.class)
    public void groupRollbackByElapse() throws Exception {
        IOException e = new IOException();
        doThrow(e).when(flusher).call();

        callByCountOrElapse = new CallByCountOrElapse(Integer.MAX_VALUE, 10L, flusher);
        newEngineAndStartup();

        String value = "value";
        engine.put(Md5Key.generate(value.getBytes()), value);
    }

    @Test(expected = IOException.class)
    public void writeFailure() throws Exception {
        doThrow(new IOException()).when(index).put(any(Md5Key.class), any(Cursor.class));

        callByCountOrElapse = new CallByCountOrElapse(Integer.MAX_VALUE, 10L, flusher);
        newEngineAndStartup();

        String value = "value";
        engine.put(Md5Key.generate(value.getBytes()), value);
    }

    private class Put implements Callable<String> {

        private final String value;

        public Put(String value) {this.value = value;}

        @Override
        public String call() throws Exception {
            return engine.put(Md5Key.generate(value.getBytes()), value);
        }
    }

    private class Remove implements Callable<String> {

        private final String value;

        public Remove(String value) {this.value = value;}

        @Override
        public String call() throws Exception {
            return engine.remove(Md5Key.generate(value.getBytes()));
        }
    }
}
