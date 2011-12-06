package com.github.zhongl.ipage;

import com.github.zhongl.util.FileBase;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkIteratorTest extends FileBase {

    @Test
    public void iterate() throws Exception {
        dir = testDir("iterate");
        // create a iPage with two chunk
        IPage iPage = IPage.baseOn(dir).build();
        for (int i = 0; i < 513; i++) {
            iPage.append(new Record(Ints.toByteArray(i)));
        }


        assertExistFile("0");
        assertExistFile("4096");

        Iterator<Record> iterator = new ChunkIterator(dir);
        for (int i = 0; i < 513; i++) {
            assertThat(iterator.next(), is(new Record(Ints.toByteArray(i))));
        }

        assertThat(iterator.hasNext(), is(false));

        iPage.close();
    }

}
