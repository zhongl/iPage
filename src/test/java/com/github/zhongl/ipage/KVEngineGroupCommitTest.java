package com.github.zhongl.ipage;

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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * More infomation about group commit is here, https://github.com/zhongl/iPage/issues/2
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class KVEngineGroupCommitTest {
    private KVEngine engine;
    private IPage ipage;
    private Index index;
    private Callable flusher;
    private Group group;

    @After
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.shutdown();
            engine.awaitForShutdown(Integer.MAX_VALUE);
        }
    }


    @Before
    public void setUp() throws Exception {
        ipage = mock(IPage.class);
        index = mock(Index.class);
        flusher = mock(Callable.class);
        group = Group.newInstance();
    }

    @Test
    public void groupCommitByCount() throws Exception {
        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(2, Long.MAX_VALUE, flusher);
        engine = new KVEngine(10L, 10, group, ipage, index, callByCountOrElapse);
        engine.startup();

        byte[] bytes = "value".getBytes();
        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<Record> putFuture = service.submit(new Put(bytes));
        Future<Record> removeFuture = service.submit(new Remove(bytes));

        putFuture.get();
        removeFuture.get();

        service.shutdown();
    }

    @Test
    public void groupRollbackByCount() throws Exception {
        IOException e = new IOException();
        doThrow(e).when(flusher).call();

        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(2, Long.MAX_VALUE, flusher);
        engine = new KVEngine(10L, 10, group, ipage, index, callByCountOrElapse);
        engine.startup();

        byte[] bytes = "value".getBytes();
        ExecutorService service = Executors.newFixedThreadPool(2);
        Future<Record> putFuture = service.submit(new Put(bytes));
        Future<Record> removeFuture = service.submit(new Remove(bytes));

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

    @Test
    public void groupCommitByElapse() throws Exception {
        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(Integer.MAX_VALUE, 10L, flusher);
        engine = new KVEngine(10L, 10, group, ipage, index, callByCountOrElapse);
        engine.startup();

        byte[] bytes = "value".getBytes();
        engine.put(Md5Key.valueOf(bytes), new Record(bytes));
    }

    @Test(expected = IOException.class)
    public void groupRollbackByElapse() throws Exception {
        IOException e = new IOException();
        doThrow(e).when(flusher).call();

        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(Integer.MAX_VALUE, 10L, flusher);
        engine = new KVEngine(10L, 10, group, ipage, index, callByCountOrElapse);
        engine.startup();

        byte[] bytes = "value".getBytes();
        engine.put(Md5Key.valueOf(bytes), new Record(bytes));
    }

    @Test(expected = IOException.class)
    public void writeFailure() throws Exception {
        doThrow(new IOException()).when(index).put(any(Md5Key.class), anyLong());

        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(Integer.MAX_VALUE, 10L, flusher);
        engine = new KVEngine(10L, 10, group, ipage, index, callByCountOrElapse);
        engine.startup();

        byte[] bytes = "value".getBytes();
        engine.put(Md5Key.valueOf(bytes), new Record(bytes));
    }

    private class Put implements Callable<Record> {

        private final byte[] bytes;

        public Put(byte[] bytes) {this.bytes = bytes;}

        @Override
        public Record call() throws Exception {
            return engine.put(Md5Key.valueOf(bytes), new Record(bytes));
        }
    }

    private class Remove implements Callable<Record> {

        private final byte[] bytes;

        public Remove(byte[] bytes) {this.bytes = bytes;}

        @Override
        public Record call() throws Exception {
            return engine.remove(Md5Key.valueOf(bytes));
        }
    }
}