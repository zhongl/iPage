package com.github.zhongl.index;

import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Files;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.TreeSet;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndicesTest extends FileTestContext {

    private Indices indices;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        dir = testDir("pages");
        File iFile = new File(dir, "123.i");

        byte[] bytes = new byte[12];
        ByteBuffer.wrap(bytes).putInt(1).putInt(2).putInt(3);
        Files.write(bytes, iFile);

        TestIndexCodec codec = new TestIndexCodec();
        indices = new Indices(iFile, codec);

    }

    @Test
    public void get() throws Exception {
        assertThat((TestIndex) indices.get(new TestKey(1)), is(new TestIndex(1, false)));
        assertThat((TestIndex) indices.get(new TestKey(2)), is(new TestIndex(2, false)));
        assertThat((TestIndex) indices.get(new TestKey(3)), is(new TestIndex(3, false)));
    }

    @Test
    public void merge() throws Exception {
        Difference difference = new Difference(new TreeSet<Index>());
        difference.add(new TestIndex(1, true));
        difference.add(new TestIndex(2, true));
        difference.add(new TestIndex(4, false));

        Indices newIndices = indices.merge(difference);

        assertThat(indices.get(new TestKey(1)), is(nullValue()));
        assertThat(indices.get(new TestKey(2)), is(nullValue()));
        assertThat((TestIndex) indices.get(new TestKey(3)), is(new TestIndex(3, false)));
        assertThat((TestIndex) indices.get(new TestKey(4)), is(new TestIndex(4, false)));

        Iterator<Index> iterator = newIndices.iterator();

        assertThat((TestIndex) iterator.next(), is(new TestIndex(3, false)));
        assertThat((TestIndex) iterator.next(), is(new TestIndex(4, false)));
        assertThat(iterator.hasNext(), is(false));
    }
}
