package com.github.zhongl.ipage;

import com.github.zhongl.util.DirBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineDataRecoveryTest extends DirBase {

    private KVEngine engine;


    @Before
    public void setUp() throws Exception {
        engine = KVEngine.baseOn(dir).build();
    }

    @After
    public void tearDown() throws Exception {
        if (engine != null) engine.shutdown();
    }

    @Test
    @Ignore("TODO")
    public void dataIsOkButDotSafeNotExist() throws Exception {
        dir = testDir("dataIsOkButDotSafeNotExist");
        engine.startup();

        Record record = new Record("value".getBytes());
        Md5Key key = Md5Key.valueOf(record);
        engine.put(key, record);
        engine.shutdown();

        boolean delete = new File(dir, ".safe").delete(); // delete .safe file
        assertThat(delete, is(true));

        engine = KVEngine.baseOn(dir).build();
        // TODO dataIsOkButDotSafeNotExist
    }
}
