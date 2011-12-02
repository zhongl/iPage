package com.github.zhongl.ipage;

import org.junit.Test;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Md5KeyTest {

    @Test(expected = IllegalArgumentException.class)
    public void invalidInitialBytesLessThan16() throws Exception {
        new Md5Key(new byte[15]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidInitialBytesGreaterThan16() throws Exception {
        new Md5Key(new byte[17]);
    }
}
