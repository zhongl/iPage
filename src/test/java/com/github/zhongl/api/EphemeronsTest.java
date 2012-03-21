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

package com.github.zhongl.api;

import com.github.zhongl.index.Key;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.FutureCallbacks;
import com.github.zhongl.util.Md5;
import com.github.zhongl.util.Nils;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class EphemeronsTest {
    private Store store;
    private Ephemerons<Integer> ephemerons;
    private Semaphore mergeBefore;
    private Semaphore mergeAfter;
    private FutureCallback<Void> ignore;

    @Before
    public void setUp() throws Exception {
        store = new Store();
        mergeBefore = new Semaphore(0);
        mergeAfter = new Semaphore(0);

        ephemerons = new Ephemerons<Integer>() {
            @Override
            protected void requestFlush(
                    final Collection<WriteOperation<Entry<Key, Integer>>> addOrUpdates,
                    final Collection<WriteOperation<Key>> removes,
                    final FutureCallback<Void> futureCallback) {

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        mergeBefore.release();
                        store.merge(addOrUpdates, removes, futureCallback);
                        mergeAfter.release();
                    }
                }).start();
            }

            @Override
            protected Integer getMiss(Key key) {
                return store.get(key);
            }
        };
        ignore = FutureCallbacks.ignore();
    }

    private void afterMerging() throws InterruptedException {mergeAfter.acquire();}

    private void beforeMerging() throws InterruptedException {mergeBefore.acquire();}

    @Test
    public void removeBeforeFlushing() throws Exception {
        Key key = key(1);
        ephemerons.throughout(10);
        ephemerons.add(key, 1, ignore);
        ephemerons.remove(key, ignore);
        ephemerons.flush();
        afterMerging();
        assertThat(store.removings.isEmpty(), is(true));
    }

    @Test
    public void removeAfterFlushing() throws Exception {
        Key key = key(1);
        ephemerons.throughout(10);
        ephemerons.remove(key, ignore);
        assertThat(ephemerons.get(key), is(nullValue()));
        ephemerons.flush();
        afterMerging();
        assertThat(store.removings, hasItem(key));
    }

    @Test
    public void removeDuringFlushing() throws Exception {
        ephemerons.throughout(5);

        Key key = key(1);
        ephemerons.add(key, 1, ignore);
        ephemerons.flush();

        beforeMerging();
        ephemerons.remove(key, ignore);
        assertThat(ephemerons.get(key), is(nullValue()));

        afterMerging();
        assertThat(store.get(key), is(1));
        assertThat(ephemerons.get(key), is(nullValue()));

        ephemerons.flush();

        afterMerging();
        assertThat(store.get(key), is(nullValue()));
        assertThat(ephemerons.get(key), is(nullValue()));
    }

    @Test
    public void updateDuringFlushing() throws Exception {
        // issue #44 : Update during flushing will lost the new value
        ephemerons.throughout(5);

        Key key = key(1);
        ephemerons.add(key, 1, ignore);
        ephemerons.flush();

        beforeMerging();
        ephemerons.add(key, 2, ignore);
        assertThat(ephemerons.get(key), is(2));

        afterMerging();
        assertThat(store.get(key), is(1));
        assertThat(ephemerons.get(key), is(2));

        ephemerons.flush();

        afterMerging();
        assertThat(store.get(key), is(2));
        assertThat(ephemerons.get(key), is(2));
    }

    @Test
    public void flowControlAndOrdering() throws Exception {
        ephemerons.throughout(4);

        for (int i = 0; i < 8; i++) ephemerons.add(key(i), i, ignore);

        afterMerging();
        assertThat(store.ordering.size(), greaterThanOrEqualTo(4));
        for (int i = 0; i < store.ordering.size(); i++) assertThat(store.ordering.get(i), is(i));

        ephemerons.flush(); // merge rest 4

        afterMerging();
        for (int i = 0; i < store.ordering.size(); i++) assertThat(store.ordering.get(i), is(i));

        assertThat(ephemerons.throughout(0), is(4));
    }

    @Test
    public void issue51() throws Exception {
        // Fixed #51 : Unexpected thoughput increment.
        ephemerons.throughout(4);

        store.setNextMerge(true);
        for (int i = 0; i < 8; i++) ephemerons.add(key(i), i, ignore);

        afterMerging();

        store.setNextMerge(true);
        ephemerons.flush(); // merge rest 4

        afterMerging();

        assertThat(ephemerons.throughout(0), is(4));

    }

    private Key key(int i) {return new Md5Key(Md5.md5((i + "").getBytes()));}

    class Store {
        final Map<Key, Integer> appendings = Collections.synchronizedMap(new HashMap<Key, Integer>());
        final Set<Key> removings = Collections.synchronizedSet(new HashSet<Key>());
        final List<Integer> ordering = Collections.synchronizedList(new ArrayList<Integer>());
        private boolean failed;

        public Integer get(Key key) {
            if (removings.contains(key)) return null;
            return appendings.get(key);
        }

        public void setNextMerge(boolean failed) {
            this.failed = failed;
        }

        public void merge(Collection<WriteOperation<Entry<Key, Integer>>> addOrUpdates,
                          Collection<WriteOperation<Key>> removes, FutureCallback<Void> futureCallback) {
            try { TimeUnit.MILLISECONDS.sleep(10L); } catch (InterruptedException e) { e.printStackTrace(); }

            if (failed) {
                onFailure(addOrUpdates);
                onFailure(removes);
                failed = true;
                futureCallback.onSuccess(Nils.VOID);
                return;
            }

            for (WriteOperation<Entry<Key, Integer>> operation : addOrUpdates) {
                Entry<Key, Integer> entry = operation.attachement();
                this.appendings.put(entry.key(), entry.value());
                ordering.add(entry.value());
            }

            for (WriteOperation<Key> operation : removes) this.removings.add(operation.attachement());
            onSuccess(addOrUpdates);
            onSuccess(removes);
            futureCallback.onSuccess(Nils.VOID);
        }

        private void onSuccess(Collection<? extends FutureCallback<Void>> callbacks) {
            for (FutureCallback<Void> callback : callbacks) callback.onSuccess(null);
        }

        private void onFailure(Collection<? extends FutureCallback<Void>> callbacks) {
            for (FutureCallback<Void> callback : callbacks) callback.onFailure(new IOException());
        }
    }

}
