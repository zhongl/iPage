package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.index.Index;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Checkpoint;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.page.DefaultCursor;
import com.github.zhongl.ex.util.Entry;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DefaultBrowserTest {

    private DefaultBrowser browser;
    private byte[] value;
    private DurableSpy durableSpy;
    private Md5Key key;
    private Index index;

    @Before
    public void setUp() throws Exception {
        value = "value".getBytes();
        durableSpy = spy(new DurableSpy());
        index = mock(Index.class);
        browser = new DefaultBrowser(index);
        key = Md5Key.generate(value);
    }

    @Test
    public void usage() throws Exception {

        // Add
        browser.update(new Entry<Md5Key, byte[]>(key, value));

        Thread.sleep(1L);


        assertThat(browser.get(key), is(value)); // get from cache


        // Merge
        browser.merge(singletonList(new Entry<Md5Key, Cursor>(key, new DefaultCursor(0L, 1))));

        Thread.sleep(5L);
        verify(index).merge(any(Iterator.class));

        assertThat(browser.get(key), is(value));
        Thread.sleep(1L);
//        assertThat(durableSpy.get, is(true));


        // Remove
        browser.update(new Entry<Md5Key, byte[]>(key, DefaultRecorder.NULL_VALUE));

        Thread.sleep(1L);
//        assertThat(durableSpy.remove, is(true));
        assertThat(browser.get(key), is(nullValue()));
    }

    @Test
    public void cache() throws Exception {
        browser.update(new Entry<Md5Key, byte[]>(key, value));
        Thread.sleep(1L);
        assertThat(browser.get(key), is(value));

        browser.update(new Entry<Md5Key, byte[]>(key, DefaultRecorder.NULL_VALUE));
        Thread.sleep(1L);
        assertThat(browser.get(key), is(nullValue()));
    }

    @After
    public void tearDown() throws Exception {
        browser.stop();
    }

    private class DurableSpy extends Actor implements Durable {

        @Override
        public void merge(Iterator<Entry<Md5Key, byte[]>> appendings, Iterator<Entry<Md5Key, Cursor>> removings, Checkpoint checkpoint) {
            // TODO merge
        }

        @Override
        public void get(Cursor cursor, FutureCallback<byte[]> callback) {
            // TODO get
        }
    }
}
