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

import com.github.zhongl.codec.Codec;
import com.github.zhongl.index.Key;
import com.github.zhongl.util.*;
import com.google.common.base.CharMatcher;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

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

        IteratorAsserts.assertIteratorOf(storage, 1, 2, 3);

        addOrUpdates = Collections.emptySet();
        removes = Arrays.asList(
                new WriteOperation<Key>(key(1), ignore)
        );

        doReturn(true).when(defragPolicy).evaluate(anyInt(), anyInt());
        storage.merge(addOrUpdates, removes, ignore);

        IteratorAsserts.assertIteratorOf(storage, 2, 3);
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

        IteratorAsserts.assertIteratorOf(storage, 4, 5);
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
        IteratorAsserts.assertIteratorOf(storage, 4);
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

    @Test
    public void issue50() throws Exception {
        // Fixed #50 : Data file has been removed unexpected

        dir = testDir("issue50");

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

        doThrow(new OutOfMemoryError()).when(defragPolicy).evaluate(anyInt(), anyInt());

        storage.merge(addOrUpdates, removes, ignore);

        String[] list;
        File pages = new File(dir, "pages");

        list = pages.list();
        assertThat(list.length, is(3));

        doReturn(true).when(defragPolicy).evaluate(anyInt(), anyInt());

        long time = System.nanoTime();

        storage.merge(addOrUpdates, removes, ignore);

        list = pages.list();
        CharMatcher matcher = CharMatcher.anyOf(".i")
                                         .or(CharMatcher.anyOf(".p"))
                                         .or(CharMatcher.anyOf(".s"));

        assertThat(list.length, is(4));

        for (String name : list) {
            String created = matcher.removeFrom(name);
            assertThat(Long.valueOf(created), greaterThan(time));
        }
    }

    private static Entry<Key, Integer> entry(int i) {
        return new Entry<Key, Integer>(key(i), i);
    }

    private static Key key(int i) {
        return new Md5Key(Md5.md5(Ints.toByteArray(i)));
    }
}
