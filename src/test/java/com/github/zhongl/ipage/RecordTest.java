package com.github.zhongl.ipage;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RecordTest {

    static Record item(String str) {
        return new Record(str.getBytes());
    }

    @Test
    public void itemToString() throws Exception {
        assertThat(new Record("item".getBytes()).toString(), is("Record{bytes=[105, 116, 101, 109]}"));
    }

}
