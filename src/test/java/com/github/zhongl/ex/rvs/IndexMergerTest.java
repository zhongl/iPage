package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.Tuple;
import com.github.zhongl.util.FileTestContext;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
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

        ReadOnlyIndex index = new ReadOnlyIndex(Arrays.asList(new Tuple(key1, new File(dir, key1.toString() + ".index"))));

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

        ReadOnlyIndex index = new ReadOnlyIndex(Arrays.asList(new Tuple(key1, new File(dir, key1.toString() + ".index"))));

        assertThat(index.entries(), hasItems(entry1, entry2, entry3));
    }
}
