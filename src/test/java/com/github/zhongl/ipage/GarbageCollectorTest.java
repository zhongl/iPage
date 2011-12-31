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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class GarbageCollectorTest {

    private ChunkList<String> chunkList;
    private GarbageCollector<String> collector;

    @Before
    public void setUp() throws Exception {
        chunkList = new ChunkList<String>(new ArrayList<Chunk<String>>());
        collector = new GarbageCollector<String>(chunkList, 16L);
    }

    @Test
    public void emptyList() throws Exception {
        assertThat(collector.collect(0L, 15L), is(0L));
    }

    @Test
    public void beginEqualEnd() throws Exception {
        chunkList.append(mockChunk(0L, 4095L));
        assertThat(collector.collect(15L, 15L), is(0L));
    }

    @Test
    public void beginGreaterThanEnd() throws Exception {
        chunkList.append(mockChunk(0L, 4095L));
        assertThat(collector.collect(15L, 7L), is(0L));
    }

    @Test
    public void lessThanMinimizeCollectLength() throws Exception {
        chunkList.append(mockChunk(0L, 4095L));
        assertThat(collector.collect(0L, 15L), is(0L));
    }

    @Test
    public void tooSmallToCollect() throws Exception {
        Chunk<String> chunk = mockChunk(0L, 4095L);
        List<Chunk<String>> pieces = Collections.singletonList(chunk);
        doReturn(pieces).when(chunk).split(0L, 32L);
        chunkList.append(chunk);
        assertThat(collector.collect(0L, 32L), is(0L));
    }

    @Test
    public void collectInOneChunk() throws Exception {
        Chunk<String> chunk = mockChunk(0L, 4095L);
        List<Chunk<String>> pieces = Arrays.asList(mockChunk(0L, 14L), mockChunk(64L, 4095));
        doReturn(pieces).when(chunk).split(15L, 64);
        chunkList.append(chunk);
        assertThat(collector.collect(15L, 64L), is(64 - 15L));
    }

    @Test
    public void collectBetweenTwoChunk() throws Exception {
        Chunk<String> chunk0 = mockChunk(0L, 4095L);
        doReturn(mockChunk(0L, 14L)).when(chunk0).left(15L);
        Chunk<String> chunk1 = mockChunk(4096L, 8191L);
        doReturn(mockChunk(4112L, 8191L)).when(chunk1).right(4112L);

        chunkList.append(chunk0);
        chunkList.append(chunk1);

        assertThat(collector.collect(15L, 4112L), is(4112 - 15L));

    }

    @Test
    public void collectHoleLeft() throws Exception {
        Chunk<String> chunk0 = mockChunk(0L, 4095L);
        doReturn(null).when(chunk0).left(15L);
        Chunk<String> chunk1 = mockChunk(4096L, 8191L);
        doReturn(mockChunk(4112L, 8191L)).when(chunk1).right(4112L);

        chunkList.append(chunk0);
        chunkList.append(chunk1);

        assertThat(collector.collect(0L, 4112L), is(4112L));
    }

    @Test
    public void endOffsetOutOfListHasOnlyElement() throws Exception {
        Chunk<String> appendingChunk = mockChunk(0L, 4096L);
        doReturn(Collections.singletonList(appendingChunk)).when(appendingChunk).split(0L, 4096L);
        chunkList.append(appendingChunk);
        assertThat(collector.collect(0L, 4096L), is(0L));
    }

    @Test
    public void beginOffsetOutOfListHasOnlyElement() throws Exception {
        Chunk<String> appendingChunk = mockChunk(16L, 4064L);
        doReturn(Collections.singletonList(appendingChunk)).when(appendingChunk).split(0L, 4096L);
        chunkList.append(appendingChunk);
        assertThat(collector.collect(0L, 4096L), is(0L));
    }

    @Test
    public void collectOnlyLeft() throws Exception {
        Chunk<String> chunk0 = mockChunk(0L, 4095L);
        doReturn(mockChunk(0L, 15L)).when(chunk0).left(15L);
        Chunk<String> chunk1 = mockChunk(4096L, 8191L);
        doReturn(chunk1).when(chunk1).right(4096L);

        chunkList.append(chunk0);
        chunkList.append(chunk1);

        assertThat(collector.collect(15L, 4096L), is(4096 - 15L));
    }

    private Chunk<String> mockChunk(long begin, long end) {
        Chunk<String> chunk0 = mock(Chunk.class);
        when(chunk0.beginPosition()).thenReturn(begin);
        when(chunk0.endPosition()).thenReturn(end);
        return chunk0;
    }

}
