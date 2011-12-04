package com.github.zhongl.ipage;

import com.github.zhongl.util.DirBase;
import org.junit.Test;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DataSecurityTest extends DirBase {

    private DataSecurity dataSecurity;


    @Test(expected = UnsafeDataStateException.class)
    public void unsafeDataState() throws Exception {
        dir = testDir("unsafeDataState");
        dir.mkdirs();
        dataSecurity = new DataSecurity(dir);
        dataSecurity.validate();
    }

    @Test
    public void safeDataState() throws Exception {
        dir = testDir("safeDataState");
        dir.mkdirs();
        dataSecurity = new DataSecurity(dir);
        dataSecurity.safeClose();
        dataSecurity.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonExistDir() throws Exception {
        dir = testDir("nonExistDir");
        new DataSecurity(dir);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidDir() throws Exception {
        dir = testDir("invalidDir");
        dir.createNewFile();
        new DataSecurity(dir);
    }
}
