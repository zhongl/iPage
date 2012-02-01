package com.github.zhongl.ex.index;

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexTest {
    @Test
    public void usage() throws Exception {
        Index index = null;

        Md5Key key = Md5Key.generate("k");
        Offset offset = new Offset(0L);

        Entry<Md5Key, Offset> entry = new Entry<Md5Key, Offset>(key, offset);
        Iterator<Entry<Md5Key, Offset>> iterator = Arrays.asList(entry).iterator();

        index.merge(iterator);

        assertThat(index.get(key), is(offset));
    }

}
