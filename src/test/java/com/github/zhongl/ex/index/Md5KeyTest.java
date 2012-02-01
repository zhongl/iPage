package com.github.zhongl.ex.index;

import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;

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
}
