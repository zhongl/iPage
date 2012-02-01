package com.github.zhongl.ex.index;

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;
import org.junit.Test;

import java.util.Iterator;

import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexTest {
    @Test
    public void usage() throws Exception {
        Index index = null;


        Iterator<Entry<Md5Key, Offset>> iterator = mock(Iterator.class);
        index.merge(iterator);
        Offset offset = index.get(Md5Key.generate("k"));
    }

}
