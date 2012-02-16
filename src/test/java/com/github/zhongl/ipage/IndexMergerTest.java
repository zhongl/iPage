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

package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.FileTestContext;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexMergerTest extends FileTestContext {

    private BigInteger one;

    @Before
    public void setUp() throws Exception {
        byte[] bytes = new byte[16];
        Arrays.fill(bytes, (byte) 0);
        ByteBuffer.wrap(bytes).put(15, (byte) 1);
        one = new BigInteger(1, bytes);
    }

    @Test
    public void defrag() throws Exception {
        dir = testDir("defrag");

        Key key1 = new Key(one);
        Range range0 = new Range(0, 0);

        Entry<Key, Range> entry1 = new Entry<Key, Range>(key1, range0);
        Entry<Key, Range> entry2 = new Entry<Key, Range>(new Key(one.add(one)), range0);
        Entry<Key, Range> entry3 = new Entry<Key, Range>(new Key(one.add(one).add(one)), Range.NIL);

        List<Entry<Key, Range>> a = Arrays.asList(entry1, entry3);
        List<Entry<Key, Range>> b = Arrays.asList(entry2);

        IndexMerger indexMerger = new IndexMerger(dir, 3) {
            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return c.value().equals(Range.NIL);
            }
        };

        indexMerger.merge(a.iterator(), b.iterator());
        Range range = new Range(7L, 14L);
        indexMerger.set(key1, range);
        indexMerger.force();

        ReadOnlyIndex index = new ReadOnlyIndex(Arrays.asList(new Entry<File, Key>(indexFile(), key1)));

        assertThat(index.entries(), hasItems(new Entry<Key, Range>(key1, range), entry2));
    }

    @Test
    public void append() throws Exception {
        dir = testDir("append");

        Key key1 = new Key(one);
        Range range0 = new Range(0, 0);

        Entry<Key, Range> entry1 = new Entry<Key, Range>(key1, range0);
        Entry<Key, Range> entry2 = new Entry<Key, Range>(new Key(one.add(one)), range0);
        Entry<Key, Range> entry3 = new Entry<Key, Range>(new Key(one.add(one).add(one)), Range.NIL);

        List<Entry<Key, Range>> a = Arrays.asList(entry1, entry3);
        List<Entry<Key, Range>> b = Arrays.asList(entry2);

        IndexMerger indexMerger = new IndexMerger(dir, 3) {
            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return false;
            }
        };

        indexMerger.merge(a.iterator(), b.iterator());
        indexMerger.force();

        ReadOnlyIndex index = new ReadOnlyIndex(Arrays.asList(new Entry<File, Key>(indexFile(), key1)));

        assertThat(index.entries(), hasItems(entry1, entry2, entry3));
    }

    private File indexFile() {
        return dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(".i");
            }
        })[0];
    }
}
