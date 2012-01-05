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

package com.github.zhongl.durable;

import com.github.zhongl.cache.Cache;
import com.github.zhongl.cache.Durable;
import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.journal.Event;
import com.github.zhongl.journal.Events;
import com.github.zhongl.page.Accessors;
import com.github.zhongl.page.Page;
import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.sequence.Sequence;
import com.github.zhongl.sequence.SequenceLoader;
import com.github.zhongl.util.FileBase;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DurableEngineTest extends FileBase {

    private DurableEngine<String> engine;

    @Test
    public void apply() throws Exception {
        dir = testDir("apply");
        List<Event> list = new ArrayList<Event>();

        final Event event0 = mock(Event.class);
        list.add(event0);
        final Event event1 = mock(Event.class);
        list.add(event1);
        final Event event2 = mock(Event.class);
        list.add(event2);

        EntryAccessor<String> entryAccessor = new EntryAccessor<String>(Accessors.STRING);
        SequenceLoader<Entry<String>> loader = new SequenceLoader<Entry<String>>(new File(dir, "seq"), entryAccessor, Cursor.NULL);
        Sequence<Entry<String>> sequence = new Sequence<Entry<String>>(loader, 16);

        Index index = new Index(new File(dir, "idx"), 1);
        Checkpoint checkpoint = new Checkpoint(new File(dir, ".cp"), 42);

        Events<Md5Key, String> events = mock(Events.class, Mockito.RETURNS_DEEP_STUBS);
        String value0 = "0";
        Md5Key key0 = Md5Key.generate(value0.getBytes());
        String value2 = "2";
        Md5Key key2 = Md5Key.generate(value2.getBytes());

        when(events.isAdd(event0)).thenReturn(true);
        when(events.getKey(event0)).thenReturn(key0);
        when(events.getValue(event0)).thenReturn(value0);

        when(events.isAdd(event1)).thenReturn(false);
        when(events.getKey(event1)).thenReturn(key0);

        when(events.isAdd(event2)).thenReturn(true);
        when(events.getKey(event2)).thenReturn(key2);
        when(events.getValue(event2)).thenReturn(value2);

        Durable<Md5Key, String> durable = new Durable<Md5Key, String>() {
            @Override
            public String load(Md5Key key) throws IOException, InterruptedException {
                return engine.load(key);
            }
        };
        long durationMilliseconds = 100L;
        Cache<Md5Key, String> cache = new Cache<Md5Key, String>(events, durable, 16, durationMilliseconds);

        engine = new DurableEngine<String>(sequence, index, checkpoint, events, false);
        engine.startup();

        Page<Event> page0 = mock(Page.class);
        doReturn(0L).when(page0).number();
        when(page0.iterator()).thenAnswer(new Answer<Iterator<Event>>() {
            @Override
            public Iterator<Event> answer(InvocationOnMock invocation) throws Throwable {
                return Collections.singletonList(event0).iterator();
            }
        });

        PageFutureCallback callback0 = new PageFutureCallback();
        engine.apply(page0, callback0);
        callback0.await();

        assertThat(cache.get(key0), is(value0));


        Page<Event> page1 = mock(Page.class);
        doReturn(21L).when(page1).number();
        when(page1.iterator()).thenAnswer(new Answer<Iterator<Event>>() {
            @Override
            public Iterator<Event> answer(InvocationOnMock invocation) throws Throwable {
                return Arrays.<Event>asList(event1, event2).iterator();
            }
        });

        PageFutureCallback callback1 = new PageFutureCallback();
        engine.apply(page1, callback1);
        callback1.await();

        Thread.sleep(durationMilliseconds);

        assertThat(cache.get(key0), is(nullValue()));
        assertThat(cache.get(key2), is(value2));

        engine.shutdown();
    }

    private static class PageFutureCallback implements FutureCallback<Page<?>> {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onSuccess(Page<?> result) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) { }

        public void await() throws InterruptedException {
            latch.await();
        }
    }
}
