package com.github.zhongl.ipage;

import org.junit.Test;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineBuilderTest {

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
