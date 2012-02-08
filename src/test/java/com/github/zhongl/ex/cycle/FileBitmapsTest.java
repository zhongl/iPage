package com.github.zhongl.ex.cycle;

import com.github.zhongl.ex.page.DefaultCursor;
import com.github.zhongl.util.FileTestContext;
import com.google.common.collect.Iterators;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileBitmapsTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");
        FileBitmaps bitmaps = new FileBitmaps(dir, 4096);

        bitmaps.merge(Iterators.singletonIterator(new DefaultCursor(15L, 1)));

        assertThat(bitmaps.nextSetBit(0L), is(15L));
        assertThat(bitmaps.nextClearBit(15L), is(16L));
    }
}
