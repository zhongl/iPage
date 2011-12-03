package com.github.zhongl.ipage;

import com.github.zhongl.util.DirBase;
import org.junit.Test;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineBuilderTest extends DirBase {

    @Test
    public void fullConfig() throws Exception {
        dir = testDir("fullConfig");
        new KVEngineBuilder(dir)
                .backlog(10)
                .initialBucketSize(256)
                .chunkCapacity(4096)
                .flushByCount(5)
                .flushByElapseMilliseconds(500L)
                .build();
    }

    @Test
    public void defaultConfig() throws Exception {
        dir = testDir("defaultConfig");
        new KVEngineBuilder(dir).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidFlushElapseMilliseconds() throws Exception {
        new KVEngineBuilder(new File(".")).flushByElapseMilliseconds(9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidFlushCount() throws Exception {
        new KVEngineBuilder(new File(".")).flushByElapseMilliseconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidChunkCapacity() throws Exception {
        new KVEngineBuilder(new File(".")).chunkCapacity(4095);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidBacklog() throws Exception {
        new KVEngineBuilder(new File(".")).backlog(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidInitialBucketSize() throws Exception {
        new KVEngineBuilder(new File(".")).initialBucketSize(0);
    }


}
