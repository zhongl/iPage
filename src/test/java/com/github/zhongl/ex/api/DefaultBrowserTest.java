package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.index.Index;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.ex.util.Entry;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static com.google.common.collect.Iterators.singletonIterator;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DefaultBrowserTest {

    private DefaultBrowser browser;
    private byte[] value;
    private DefaultBrowserTest.StoreSpy storeSpy;
    private Md5Key key;
    private Index index;

    @Before
    public void setUp() throws Exception {
        value = "value".getBytes();
        storeSpy = new StoreSpy();
        index = mock(Index.class);
        browser = new DefaultBrowser(index);
        key = Md5Key.generate(value);
    }

    @Test
    public void usage() throws Exception {

        // Add
        browser.update(new Revision(0L), new Entry<Md5Key, byte[]>(key, value));

        Thread.sleep(1L);

        assertThat(storeSpy.append, is(true));
        assertThat(browser.get(key), is(value)); // get from cache


        // Merge
        browser.merge(singletonList(new Entry<Md5Key, Offset>(key, new Offset(0L))));

        Thread.sleep(5L);
        verify(index).merge(any(Iterator.class));

        assertThat(browser.get(key), is(value));
        Thread.sleep(1L);
        assertThat(storeSpy.get, is(true));


        // Iterate
        Iterator<byte[]> iterator = browser.iterator();
        Thread.sleep(1L);
        assertThat(iterator.next(), is(value));


        // Remove
        browser.update(new Revision(1L), new Entry<Md5Key, byte[]>(key, DefaultRecorder.NULL_VALUE));

        Thread.sleep(1L);
        assertThat(storeSpy.remove, is(true));
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

    private class StoreSpy extends Actor implements Store {

        private volatile boolean append;
        private volatile boolean remove;
        private volatile boolean get;

        @Override
        public void append(Revision revision, Entry<Md5Key, byte[]> entry) {
            append = true;
        }

        @Override
        public void remove(Revision revision, Offset offset) {
            remove = true;
        }

        @Override
        public void get(Offset offset, FutureCallback<byte[]> callback) {
            get = true;
            callback.onSuccess(value);
        }

        @Override
        public void iterator(FutureCallback<Iterator<byte[]>> callback) {
            callback.onSuccess(singletonIterator(value));
        }
    }
}
