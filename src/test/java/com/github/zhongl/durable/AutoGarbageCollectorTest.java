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

import com.github.zhongl.sequence.Cursor;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class AutoGarbageCollectorTest {

    private final ExecutorService service = Executors.newSingleThreadExecutor();

    @Test
    public void collectFirstThree() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FakeCollectable collectable = new FakeCollectable(new String[] {null, null, null, ""}, latch);
        AutoGarbageCollector agc = new AutoGarbageCollector(collectable);
        agc.start();
        latch.await();
        agc.stop();
        collectable.assertCollected(0L, 3L);
    }

    @Test
    public void collectMiddleTwo() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FakeCollectable collectable = new FakeCollectable(new String[] {"", null, null, ""}, latch);
        AutoGarbageCollector agc = new AutoGarbageCollector(collectable);
        agc.start();
        latch.await();
        agc.stop();
        collectable.assertCollected(1L, 3L);
    }

    @Test
    public void collectAll() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FakeCollectable collectable = new FakeCollectable(new String[] {null, null, null, null}, latch);
        AutoGarbageCollector agc = new AutoGarbageCollector(collectable);
        agc.start();
        latch.await();
        agc.stop();
        collectable.assertCollected(0L, 4L);
    }

    @Test
    public void collectLastOne() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FakeCollectable collectable = new FakeCollectable(new String[] {"", "", "", null}, latch);
        AutoGarbageCollector agc = new AutoGarbageCollector(collectable);
        agc.start();
        latch.await();
        agc.stop();
        collectable.assertCollected(3L, 4L);
    }

    @Test
    public void collectOnlyOne() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FakeCollectable collectable = new FakeCollectable(new String[] {null}, latch);
        AutoGarbageCollector agc = new AutoGarbageCollector(collectable);
        agc.start();
        latch.await();
        agc.stop();
        collectable.assertCollected(0L, 1L);
    }

    @Test
    public void collectTwice() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        FakeCollectable collectable = new FakeCollectable(new String[] {null, "", null, null}, latch, 2);
        AutoGarbageCollector agc = new AutoGarbageCollector(collectable);
        agc.start();
        latch.await();
        agc.stop();
        collectable.assertCollected(2L, 4L);
    }


    @After
    public void tearDown() throws Exception {
        service.shutdown();
    }

    private class FakeCollectable implements Collectable<String> {
        private final CountDownLatch latch;
        private final int minimizeCollectedLength;
        private volatile Cursor begin;
        private volatile Cursor end;
        private final String[] data;

        public FakeCollectable(String[] data, CountDownLatch latch) {
            this(data, latch, 1);
        }

        public FakeCollectable(String[] data, CountDownLatch latch, int minimizeCollectedLength) {
            this.data = data;
            this.latch = latch;
            this.minimizeCollectedLength = minimizeCollectedLength;
        }

        public void assertCollected(long begin, long end) {
            assertThat(this.begin, is(new Cursor(begin)));
            assertThat(this.end, is(new Cursor(end)));
        }

        @Override
        public boolean getAndNext(final Cursor cursor, final FutureCallback<ValueAndNextCursor<String>> cursorFutureCallback) {
            service.submit(new Runnable() {
                @Override
                public void run() {
                    String value = data[((int) cursor.offset())];
                    Cursor next = cursor.forword(1);
                    cursorFutureCallback.onSuccess(new ValueAndNextCursor<String>(value, next));
                }
            });
            return true;
        }

        @Override
        public boolean garbageCollect(final Cursor begin, final Cursor end, final FutureCallback<Long> callback) {
            service.submit(new Runnable() {
                @Override
                public void run() {
                    if (end.distanceTo(begin) < minimizeCollectedLength) {
                        callback.onSuccess(0L);
                    } else {
                        FakeCollectable.this.begin = begin;
                        FakeCollectable.this.end = end;
                        callback.onSuccess((long) end.distanceTo(begin));
                        latch.countDown();
                    }
                }
            });
            return true;
        }

        @Override
        public boolean isTail(Cursor cursor) {
            return cursor.offset() >= data.length;
        }

    }
}
