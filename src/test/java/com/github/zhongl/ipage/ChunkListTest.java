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
import org.mockito.Matchers;

import java.io.File;
import java.io.IOException;

import static com.github.zhongl.ipage.Builder.ChunkFactory;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkListTest extends FileBase {
    private ChunkFactory chunkFactory;
    private ChunkList<String> chunkList;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        chunkFactory = mock(ChunkFactory.class);
    }

    @Test
    public void last() throws Exception {
        dir = testDir("last");
        doReturn(mock(Chunk.class)).when(chunkFactory).create(Matchers.anyLong(), Matchers.any(File.class));
        newChunkList();
        Chunk<String> chunk = chunkList.last();
        assertThat(chunk, is(notNullValue()));
        assertThat(chunk, is(chunkList.last()));
    }

    @Test
    public void chunkInOffset() throws Exception {
        dir = testDir("last");
        Chunk chunk = mock(Chunk.class);
        doReturn(0L).when(chunk).beginPositionInIPage();
        doReturn(4095L).when(chunk).endPositionInIPage();
        doReturn(chunk).when(chunkFactory).create(Matchers.anyLong(), Matchers.any(File.class));
        newChunkList();
        assertThat(chunkList.chunkIn(0L), is(chunk));
    }

    @Test
    public void garbageCollectBetweenTwo() throws Exception {
        dir = testDir("garbageCollectBetweenTwo");
        dir.mkdirs();
        chunkFactory = new ChunkFactory<String>(4096L, CommonAccessors.STRING);
        newChunkList();

        ChunkBase.fullFill(chunkList.last());
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last());

        assertExistFile("0");
        assertExistFile("4096");

        long collected = chunkList.garbageCollect(16L, 4096 + 32L);
        assertThat(collected, is(4096 + 16L));

        assertThat(new File(dir, "0").length(), is(16L));
        assertNotExistFile("4096");
        assertThat(new File(dir, "4128").length(), is(4096 - 32L));
    }

    @Test
    public void garbageCollectLeft() throws Exception {
        dir = testDir("garbageCollectLeft");
        dir.mkdirs();
        chunkFactory = new ChunkFactory<String>(4096L, CommonAccessors.STRING);
        newChunkList();

        ChunkBase.fullFill(chunkList.last());
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last());

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
        chunkFactory = new ChunkFactory<String>(8192L, CommonAccessors.STRING);
        newChunkList();

        ChunkBase.fullFill(chunkList.last());
        ChunkBase.fullFill(chunkList.last());
        chunkList.grow();
        ChunkBase.fullFill(chunkList.last());
        ChunkBase.fullFill(chunkList.last());

        assertExistFile("0");
        assertExistFile("8192");

        long collected = chunkList.garbageCollect(0L, 4096 + 16L);
        assertThat(collected, is(4096 + 16L));

        assertNotExistFile("0");
        assertThat(new File(dir, "4112").length(), is(4096 - 16L));
        assertThat(new File(dir, "8192").length(), is(8192L));
    }

    private void newChunkList() throws IOException {chunkList = new ChunkList<String>(dir, chunkFactory);}
}
