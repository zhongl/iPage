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

package com.github.zhongl.journal;

import com.github.zhongl.cache.Cache;
import com.github.zhongl.durable.DurableEngine;
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class JournalTest extends FileBase {

    private Journal journal;

    @Test
    public void append() throws Exception {
        dir = testDir("append");

        boolean groupCommit = false;
        long flushElapseMilliseconds = Long.MAX_VALUE;
        int flushCount = 1;
        DurableEngine durableEngine = mock(DurableEngine.class);

        Cache cache = mock(Cache.class);

        journal = new Journal(dir, new EventAccessor(), durableEngine, cache, flushCount, flushElapseMilliseconds, groupCommit);

        journal.open();

        MockEvent event = new MockEvent("event");
        journal.append(event);
        event.await();

        verify(cache, times(1)).apply(event);

        ArgumentCaptor<EventPage> argumentCaptor = ArgumentCaptor.forClass(EventPage.class);
        verify(durableEngine).apply(argumentCaptor.capture());
        assertThat((MockEvent) argumentCaptor.getValue().iterator().next(), sameInstance(event));

        journal.close();
    }

    @Test
    public void load() throws Exception {
        dir = testDir("load");

        EventAccessor accessor = new EventAccessor();
        new EventPage(new File(dir, "0"), accessor).fix();
        new File(dir, "1").createNewFile();

        boolean groupCommit = false;
        long flushElapseMilliseconds = Long.MAX_VALUE;
        int flushCount = 1;
        DurableEngine durableEngine = mock(DurableEngine.class);

        Cache cache = mock(Cache.class);

        journal = new Journal(dir, new EventAccessor(), durableEngine, cache, flushCount, flushElapseMilliseconds, groupCommit);

        journal.open();

        ArgumentCaptor<EventPage> argumentCaptor = ArgumentCaptor.forClass(EventPage.class);
        verify(durableEngine).apply(argumentCaptor.capture());
        assertThat(argumentCaptor.getAllValues().size(), is(1));
        assertThat(argumentCaptor.getValue().number(), is(0L));

        MockEvent event = new MockEvent("event");
        journal.append(event);
        event.await();

        verify(durableEngine, times(2)).apply(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().number(), is(1L));
    }


    @Override
    @After
    public void tearDown() throws Exception {
        journal.close();
        super.tearDown();
    }

    private static class MockEvent extends StringEvent {
        private final CountDownLatch latch = new CountDownLatch(1);

        public MockEvent(String value) {
            super(value);
        }

        public void await() throws InterruptedException {
            latch.await();
        }

        @Override
        public void onSuccess(Void result) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            fail(t.toString());
        }
    }
}
