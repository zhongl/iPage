/*
 * Copyright 2011 zhongl
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.kvengine;

import com.github.zhongl.util.FileBase;
import org.junit.Test;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineBuilderTest extends FileBase {

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
