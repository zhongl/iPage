package com.github.zhongl.ex.index;

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.util.FileTestContext;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexTest extends FileTestContext {

    private Index index;

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");
        index = new FlexIndex(dir);

        int a1 = 12;
        int a2 = 34;

        List<Entry<Md5Key, Offset>> entries = asList(
                entry(7),
                entry(a1),
                entry(a2)
        );

        Collections.sort(entries);

        index.merge(entries.iterator());

        assertThat(index.get(key(7)), is(value(7)));
        assertThat(index.get(key(a1)), is(value(a1)));
        assertThat(index.get(key(a2)), is(value(a2)));

        entries = asList(
                entry(7, Offset.NIL),      // remove
                entry(14),                 // add
                entry(a1, new Offset(a2))  // update
        );

        Collections.sort(entries);

        index.merge(entries.iterator());

        assertThat(index.get(key(7)), is(nullValue()));
        assertThat(index.get(key(a1)), is(value(a2)));
        assertThat(index.get(key(a2)), is(value(a2)));
    }

    @Test
    public void mergeABandLeftA() throws Exception {
        dir = testDir("mergeABandLeftA");
        index = new FlexIndex(dir);

        index.merge(asList(entry(1), entry(4), entry(5)).iterator()); // A
        index.merge(asList(entry(2), entry(3)).iterator()); // B
    }

    @Test
    public void mergeABandLeftOnlyOneA() throws Exception {
        dir = testDir("mergeABandLeftOnlyOneA");
        index = new FlexIndex(dir);

        index.merge(asList(entry(1), entry(4)).iterator()); // A
        index.merge(asList(entry(2), entry(3)).iterator()); // B
    }

    @Test
    public void mergeABandLeftB() throws Exception {
        dir = testDir("mergeABandLeftB");
        index = new FlexIndex(dir);

        index.merge(asList(entry(1), entry(2)).iterator()); // A
        index.merge(asList(entry(3), entry(4), entry(5)).iterator()); // B
    }

    @Test
    public void mergeEmpty() throws Exception {
        dir = testDir("mergeEmpty");
        index = new FlexIndex(dir);
        index.merge(Collections.<Entry<Md5Key, Offset>>emptyList().iterator());
    }

    @Test
    public void getNoExistKey() throws Exception {
        dir = testDir("getNoExistKey");
        index = new FlexIndex(dir);

        assertThat(index.get(key(1)), is(nullValue()));

        index.merge(Collections.singletonList(entry(2)).iterator());

        assertThat(index.get(key(1)), is(nullValue()));
    }

    @Test
    public void load() throws Exception {
        dir = testDir("load");
        index = new FlexIndex(dir);
        index.merge(Collections.singletonList(entry(1)).iterator());
        index.close();

        File one = new File(dir, "1");
        one.mkdir();
        new FlexIndex(dir); // load 0 and remove 1

        assertThat(one.exists(), is(false));
    }

    @Test
    public void overflow() throws Exception {
        dir = testDir("overflow");
        index = new FlexIndex(dir);

        int capacity = FlexIndex.MAX_ENTRY_SIZE + 2; // to append more than one page.
        List<Entry<Md5Key, Offset>> entries = new ArrayList<Entry<Md5Key, Offset>>(capacity);
        for (int i = 0; i < capacity; i++) entries.add(entry(i));
        Collections.sort(entries);

        index.merge(entries.iterator());

        assertThat(index.get(key(FlexIndex.MAX_ENTRY_SIZE + 1)), is(value(FlexIndex.MAX_ENTRY_SIZE + 1)));

    }

    private Entry<Md5Key, Offset> entry(int i, Offset o) {
        return new Entry<Md5Key, Offset>(key(i), o);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        index.close();
    }

    private Entry<Md5Key, Offset> entry(int i) {return new Entry<Md5Key, Offset>(key(i), value(i));}

    private Offset value(long v) {return new Offset(v);}

    private Md5Key key(int k) {
        return Md5Key.generate(k + "");
    }

}
