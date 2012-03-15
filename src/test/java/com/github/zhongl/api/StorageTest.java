/*
 * Copyright 2012 zhongl
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

import com.github.zhongl.codec.Codec;
import com.github.zhongl.index.Key;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.FutureCallbacks;
import com.github.zhongl.util.Md5;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StorageTest extends FileTestContext {
    private FutureCallback<Void> ignore;
    private DefragPolicy defragPolicy;
    private Codec<Entry<Key, Integer>> entryCodec;
    private RangeIndexCodec indexCodec;


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Md5KeyCodec keyCodec = new Md5KeyCodec();

        ignore = FutureCallbacks.ignore();
        defragPolicy = mock(DefragPolicy.class);

        indexCodec = new RangeIndexCodec(keyCodec);
        entryCodec = new EntryCodec<Integer>(keyCodec, new Codec<Integer>() {
            @Override
            public Integer decode(ByteBuffer byteBuffer) {
                return byteBuffer.getInt();
            }

            @Override
            public ByteBuffer encode(Integer value) {
                return (ByteBuffer) ByteBuffer.allocate(4).putInt(value).flip();
            }
        });
    }

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");

        Snapshot<Integer> snapshot = new Snapshot<Integer>(dir, indexCodec, entryCodec);
        Storage<Integer> storage = new Storage<Integer>(snapshot, indexCodec, defragPolicy);

        Collection<WriteOperation<Entry<Key, Integer>>> addOrUpdates;
        Collection<WriteOperation<Key>> removes;

        addOrUpdates = Arrays.asList(
                new WriteOperation<Entry<Key, Integer>>(entry(1), ignore),
                new WriteOperation<Entry<Key, Integer>>(entry(2), ignore),
                new WriteOperation<Entry<Key, Integer>>(entry(3), ignore)
        );

        removes = Collections.emptySet();

        storage.merge(addOrUpdates, removes, ignore);

        assertThat(storage.get(key(1)), is(1));
        assertThat(storage.get(key(2)), is(2));
        assertThat(storage.get(key(3)), is(3));

        assertIteratorOf(storage, 1, 2, 3);

        addOrUpdates = Collections.emptySet();
        removes = Arrays.asList(
                new WriteOperation<Key>(key(1), ignore)
        );

        doReturn(true).when(defragPolicy).evaluate(anyInt(), anyInt());
        storage.merge(addOrUpdates, removes, ignore);

        assertIteratorOf(storage, 2, 3);
    }

    @Test
    public void issue36() throws Exception {
        // Fixed #36 : Appendings should not be removed during defragment.

        dir = testDir("issue36");

        Snapshot<Integer> snapshot = new Snapshot<Integer>(dir, indexCodec, entryCodec);
        Storage<Integer> storage = new Storage<Integer>(snapshot, indexCodec, defragPolicy);

        Collection<WriteOperation<Entry<Key, Integer>>> addOrUpdates;
        Collection<WriteOperation<Key>> removes;

        addOrUpdates = Arrays.asList(
                new WriteOperation<Entry<Key, Integer>>(entry(1), ignore),
                new WriteOperation<Entry<Key, Integer>>(entry(2), ignore),
                new WriteOperation<Entry<Key, Integer>>(entry(3), ignore)
        );

        removes = Collections.emptySet();

        doReturn(true).when(defragPolicy).evaluate(anyInt(), anyInt());
        storage.merge(addOrUpdates, removes, ignore);

        addOrUpdates = Arrays.asList(
                new WriteOperation<Entry<Key, Integer>>(entry(4), ignore),
                new WriteOperation<Entry<Key, Integer>>(entry(5), ignore)
        );

        removes = Arrays.asList(
                new WriteOperation<Key>(key(1), ignore),
                new WriteOperation<Key>(key(2), ignore),
                new WriteOperation<Key>(key(3), ignore)
        );

        storage.merge(addOrUpdates, removes, ignore);

        assertIteratorOf(storage, 4, 5);
    }

    @Test
    public void issue38() throws Exception {
        // Fixed #38 : Multi-version value

        dir = testDir("issue38");

        Snapshot<Integer> snapshot = new Snapshot<Integer>(dir, indexCodec, entryCodec);
        Storage<Integer> storage = new Storage<Integer>(snapshot, indexCodec, defragPolicy);

        Collection<WriteOperation<Entry<Key, Integer>>> addOrUpdates;
        Collection<WriteOperation<Key>> removes;

        addOrUpdates = Arrays.asList(
                new WriteOperation<Entry<Key, Integer>>(entry(3), ignore)
        );

        removes = Collections.emptySet();

        storage.merge(addOrUpdates, removes, ignore);

        addOrUpdates = Arrays.asList(
                new WriteOperation<Entry<Key, Integer>>(new Entry<Key, Integer>(key(3), 4), ignore)
        );

        storage.merge(addOrUpdates, removes, ignore);

        assertThat(storage.get(key(3)), is(4));
        assertIteratorOf(storage, 4);
    }


    @Test
    public void issue42() throws Exception {
        // Fixed #42 : Duplicate key can cause value missing

        dir = testDir("issue42");

        Snapshot<Integer> snapshot = new Snapshot<Integer>(dir, indexCodec, entryCodec);
        Storage<Integer> storage = new Storage<Integer>(snapshot, indexCodec, defragPolicy);

        Collection<WriteOperation<Entry<Key, Integer>>> addOrUpdates;
        Collection<WriteOperation<Key>> removes;

        addOrUpdates = Arrays.asList(
                new WriteOperation<Entry<Key, Integer>>(entry(3), ignore)
        );

        removes = Collections.emptySet();

        storage.merge(addOrUpdates, removes, ignore);
        storage.merge(addOrUpdates, removes, ignore);
        storage.merge(addOrUpdates, removes, ignore);

        assertThat(storage.get(key(3)), is(3));
    }


    private static <V> void assertIteratorOf(Iterable<V> iterable, V... values) {
        Iterator<V> iterator = iterable.iterator();
        for (V value : values) assertThat(iterator.next(), is(value));
        assertThat(iterator.hasNext(), is(false));
    }

    private static Entry<Key, Integer> entry(int i) {
        return new Entry<Key, Integer>(key(i), i);
    }

    private static Key key(int i) {
        return new Md5Key(Md5.md5(Ints.toByteArray(i)));
    }
}
