package com.github.zhongl.ipage;

import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexTest extends DirBase {

    private Index index;

    @After
    public void tearDown() throws Exception {
        if (index != null) index.close();
    }

    @Test(expected = IllegalStateException.class)
    public void repeatSetInitBucketSize() throws Exception {
        dir = testDir("repeatSetInitBucketSize");
        Index.baseOn(dir).initBucketSize(1).initBucketSize(1);
    }

    @Test
    public void grow() throws Exception {
        dir = testDir("grow");
        index = Index.baseOn(dir).initBucketSize(1).build();

        for (int i = 0; i < 163; i++) {
            index.put(Md5Key.valueOf(Ints.toByteArray(i)), 7L);
        }

        assertExistFile("0");
        assertNotExistFile("1");

        index.put(Md5Key.valueOf(Ints.toByteArray(163)), 7L);

        assertExistFile("0");
        assertExistFile("1");
    }

    @Test
    public void loadExist() throws Exception {
        dir = testDir("loadExist");
        index = Index.baseOn(dir).initBucketSize(1).build();

        for (int i = 0; i < 164; i++) {
            index.put(Md5Key.valueOf(Ints.toByteArray(i)), 7L);
        }

        index.close();

        index = Index.baseOn(dir).initBucketSize(1).build();

        assertThat(index.get(Md5Key.valueOf(Ints.toByteArray(0))), is(7L));
        assertThat(index.get(Md5Key.valueOf(Ints.toByteArray(162))), is(7L));
        assertThat(index.get(Md5Key.valueOf(Ints.toByteArray(163))), is(7L));
    }
}
