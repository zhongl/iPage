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

package com.github.zhongl.ipage;

import com.github.zhongl.accessor.CommonAccessors;
import com.github.zhongl.util.FileBase;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkListTest extends FileBase {
    private ChunkList<String> chunkList;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void last() throws Exception {
        dir = testDir("last");
        dir.mkdirs();
        newChunkList();
        Chunk<String> chunk = chunkList.last();
        assertThat(chunk, is(notNullValue()));
        assertThat(chunk, is(chunkList.last()));
    }

    @Test
    public void garbageCollectBetweenTwo() throws Exception {
        dir = testDir("garbageCollectBetweenTwo");
        dir.mkdirs();
        newChunkList();

        ChunkBase.fullFill(chunkList.last()); // read only chunk
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last()); // read only chunk
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last()); // appending chunk

        assertExistFile("0");
        assertExistFile("4096");

        long collected = chunkList.garbageCollect(32L, 4096 + 64L);
        assertThat(collected, is(4096 + 32L));

        assertThat(new File(dir, "0").length(), is(32L));
        assertNotExistFile("4096");
        assertThat(new File(dir, "4160").length(), is(4096 - 64L));
    }

    @Test
    public void garbageCollectLeft() throws Exception {
        dir = testDir("garbageCollectLeft");
        dir.mkdirs();
        newChunkList();

        ChunkBase.fullFill(chunkList.last()); // read only chunk
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last()); // read only chunk
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last()); // appending chunk

        assertExistFile("0");
        assertExistFile("4096");

        long collected = chunkList.garbageCollect(0L, 4096L);
        assertThat(collected, is(4096L));

        assertNotExistFile("0");
        assertThat(new File(dir, "4096").length(), is(4096L));
    }

    @Test
    public void garbageCollectInOneChunk() throws Exception {
        dir = testDir("garbageCollectInOneChunk");
        dir.mkdirs();
        chunkList = new ChunkList<String>(dir, 4096, CommonAccessors.STRING, 32);

        ChunkBase.fullFill(chunkList.last());
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last());

        assertExistFile("0");
        assertExistFile("4096");

        long collected = chunkList.garbageCollect(0L, 4080);
        assertThat(collected, is(4080L));

        assertNotExistFile("0");
        assertThat(new File(dir, "4080").length(), is(16L));
        assertThat(new File(dir, "4096").length(), is(4096L));
    }

    private void newChunkList() throws IOException {
        chunkList = new ChunkList<String>(dir, 4096, CommonAccessors.STRING, 4096);
    }
}
