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

    @Test
    public void parse() throws Exception {
        Md5Key key = new Md5Key("b8d1b43eae73587ba56baef574709ecb");
        assertThat(key, is(Md5Key.generate("hex")));
    }

    @Test
    public void min() throws Exception {
        assertThat(Md5Key.MIN.toString(), is("00000000000000000000000000000000"));
        assertThat(Md5Key.MIN.bigInteger.intValue(), is(0));
    }
}
