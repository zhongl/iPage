package com.github.zhongl.ipage;

import com.google.common.base.Joiner;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class DirBase {
    private static final String BASE_ROOT = "target/tmpTestFiles/";
    protected File dir;

    @Before
    public void setUp() throws Exception {
        new File(BASE_ROOT).mkdirs();
    }

    private void delete(File file) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) delete(f);
            if (!file.delete()) throw new IOException("Can't delete dir " + file);
        }
        if (file.isFile() && !file.delete()) throw new IOException("Can't delete file " + file);
    }

    protected File testDir(String name) throws IOException {
        File file = new File(BASE_ROOT, Joiner.on('.').join(getClass().getSimpleName(), name));
        if (file.exists()) delete(file);
        return file;
    }

    protected void assertNotExistFile(String name) {assertThat(new File(dir, name).exists(), is(false));}

    protected void assertExistFile(String name) {assertThat(new File(dir, name).exists(), is(true));}
}
