package com.github.zhongl.ipage;

import com.github.zhongl.util.DirBase;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Callable;

import static com.github.zhongl.ipage.RecordTest.item;
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

        Record record = item("record");
        Md5Key key = Md5Key.valueOf(record);

        assertThat(engine.put(Md5Key.valueOf(record), record), is(nullValue()));

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

        IPage ipage = mock(IPage.class);
        Index index = mock(Index.class);
        int count = 2;
        long elapseMilliseconds = 100L;
        Callable<?> flusher = mock(Callable.class);

        engine = new KVEngine(10L, 10, Group.NULL, ipage, index, new CallByCountOrElapse(count, elapseMilliseconds, flusher));
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

        IPage ipage = mock(IPage.class);
        Index index = mock(Index.class);
        int count = 2;
        long elapseMilliseconds = 100L;
        Callable<?> flusher = mock(Callable.class);

        engine = new KVEngine(10L, 10, Group.NULL, ipage, index, new CallByCountOrElapse(count, elapseMilliseconds, flusher));
        engine.startup();

        engine.put(mock(Md5Key.class), mock(Record.class));
        Thread.sleep(elapseMilliseconds);                   // flush by elapse and reset
        engine.put(mock(Md5Key.class), mock(Record.class));

        verify(flusher, times(1)).call();
    }

}