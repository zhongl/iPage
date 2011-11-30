package com.github.zhongl.ipage;

import com.google.common.base.Joiner;
import org.junit.After;
import org.junit.Before;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class FileBase {
    private static final String BASE_ROOT = "target/tmpTestFiles/";
    protected File file;

    @Before
    public void setUp() throws Exception {
        new File(BASE_ROOT).mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        if (file != null && file.exists()) file.delete();
    }

    protected File testFile(String name) {
        return new File(BASE_ROOT, Joiner.on('.').join(getClass().getSimpleName(), name));
    }
}
