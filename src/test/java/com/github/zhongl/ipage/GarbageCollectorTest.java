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

import com.github.zhongl.util.FileBase;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class GarbageCollectorTest extends FileBase {

    /*
    @Test
    public void collect() throws Exception {
        file = testFile("collect");
        GarbageCollector<String> collector = new GarbageCollector<String>(chunkList, minimizeCollectLength);
        Chunk<String> chunk = mock(Chunk.class);
        when(chunk.beginPosition()).thenReturn(0L);

        ChunkList<String> chunkList = mock(ChunkList.class);
        doReturn(chunk).when(chunkList).first();

        collector.collect(64L, chunkList);

        verify(chunkList).garbageCollect(0L, 64L);

        collector.collect(128L, chunkList);

        verify(chunkList).garbageCollect(64L, 128L);

        collector.collect(16L, chunkList);

        verify(chunkList).garbageCollect(0L, 16L);

        reset(chunkList);
        long collect = collector.collect(16L, chunkList);
        assertThat(collect, is(0L));

        verify(chunkList, never()).garbageCollect(anyLong(), anyLong());
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
    public void garbageCollectInAppendingChunk() throws Exception {
        dir = testDir("garbageCollectInAppendingChunk");
        dir.mkdirs();
        newChunkList();

        ChunkBase.fullFill(chunkList.last()); // read only chunk

        assertExistFile("0");

        long collected = chunkList.garbageCollect(0L, 64L);
        assertThat(collected, is(0L));

    }

    @Test
    public void garbageCollectInOneChunk() throws Exception {
        dir = testDir("garbageCollectInOneChunk");
        dir.mkdirs();
        newChunkList();

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

    * */
}
