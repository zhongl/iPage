package com.github.zhongl.ex.index;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Md5KeyTest {
    @Test
    public void hex() throws Exception {
        String hex = Md5Key.generate("hex").toString();
        assertThat(hex, is("b8d1b43eae73587ba56baef574709ecb"));
    }
}
