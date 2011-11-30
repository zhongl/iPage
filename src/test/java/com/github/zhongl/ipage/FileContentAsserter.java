package com.github.zhongl.ipage;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class FileContentAsserter {

    private final byte[] content;

    public static FileContentAsserter of(File file) throws IOException {
        return new FileContentAsserter(file);
    }

    public FileContentAsserter(File file) throws IOException {
        content = Files.toByteArray(file);
    }

    public void assertIs(byte[] expect) {
        byte[] actual = new byte[expect.length];
        System.arraycopy(content, 0, actual, 0, actual.length);
        assertThat(actual, is(expect));
    }

}
