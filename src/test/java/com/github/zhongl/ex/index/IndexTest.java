package com.github.zhongl.ex.index;

import com.github.zhongl.ex.page.Batch;
import com.github.zhongl.ex.page.Offset;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexTest {
    @Test
    public void usage() throws Exception {
        Index index = null;


        Batch batch = mock(Batch.class);
        index.merge(batch);
        Offset offset = index.get(Md5Key.generate("k"));
    }
}
