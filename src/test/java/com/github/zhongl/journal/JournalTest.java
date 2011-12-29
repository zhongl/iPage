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

import com.github.zhongl.durable.DurableEngine;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class JournalTest {

    private Journal journal;

    @Test
    public void append() throws Exception {
        boolean groupCommit = false;
        long flushElapseMilliseconds = Long.MAX_VALUE;
        int flushCount = 1;
        DurableEngine durableEngine = mock(DurableEngine.class);

        Page page = mock(Page.class);
        PageRepository pageRepository = mock(PageRepository.class);
        doReturn(Collections.emptyList()).when(pageRepository).unappliedPages();
        doReturn(page).when(pageRepository).create();

        journal = new Journal(pageRepository, durableEngine, flushCount, flushElapseMilliseconds, groupCommit);
        verify(pageRepository, times(1)).create();

        journal.open();
        verify(pageRepository, times(1)).unappliedPages();

        MockEvent event = new MockEvent();
        journal.append(event);
        event.await();

        verify(page, times(1)).fix();
        verify(durableEngine).apply(page);
        verify(pageRepository, times(2)).create();

        journal.close();
        verify(page, times(2)).fix();
    }

    private static class MockEvent implements Event {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onCommit() {
            latch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            fail(t.toString());
        }

        @Override
        public int compareTo(Event o) {
            return 0;
        }

        public void await() throws InterruptedException {
            latch.await();
        }
    }
}
