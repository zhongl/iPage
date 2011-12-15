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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkListTest {
    private ChunkList<String> chunkList;

    @Before
    public void setUp() throws Exception {
        chunkList = new ChunkList<String>(new ArrayList<Chunk<String>>());
    }

    @Test
    public void isEmpty() throws Exception {
        assertThat(chunkList.isEmpty(), is(true));
        chunkList.append(mock(Chunk.class));
        assertThat(chunkList.isEmpty(), is(false));
    }

    @Test
    public void close() throws Exception {
        Chunk chunk0 = mock(Chunk.class);
        chunkList.append(chunk0);

        chunkList.close();
        verify(chunk0).close();
    }

    @Test
    public void last() throws Exception {
        try {
            chunkList.last();
            fail("Should index out of bound.");
        } catch (IndexOutOfBoundsException e) { }

        Chunk chunk0 = mock(Chunk.class);
        chunkList.append(chunk0);
        assertThat(chunkList.last(), is(chunk0));

        Chunk chunk1 = mock(Chunk.class);
        chunkList.append(chunk1);
        assertThat(chunkList.last(), is(chunk1));
    }

    @Test
    public void first() throws Exception {
        try {
            chunkList.first();
            fail("Should index out of bound.");
        } catch (IndexOutOfBoundsException e) { }

        Chunk chunk0 = mock(Chunk.class);
        chunkList.append(chunk0);
        assertThat(chunkList.first(), is(chunk0));

        Chunk chunk1 = mock(Chunk.class);
        chunkList.append(chunk1);
        assertThat(chunkList.first(), is(chunk0));
    }

    @Test
    public void insertAndGetAndSetAndRemove() throws Exception {
        Chunk chunk0 = mock(Chunk.class);
        Chunk chunk1 = mock(Chunk.class);
        chunkList.insert(0, chunk0);
        assertThat(chunkList.get(0), is(chunk0));
        assertThat(chunkList.set(0, chunk1), is(chunk0));
        assertThat(chunkList.remove(0), is(chunk1));
    }

    @Test
    public void chunkIn() throws Exception {
        Chunk chunk0 = mock(Chunk.class);
        when(chunk0.beginPosition()).thenReturn(0L);
        when(chunk0.endPosition()).thenReturn(15L);
        Chunk chunk1 = mock(Chunk.class);
        when(chunk1.beginPosition()).thenReturn(16L);
        when(chunk1.endPosition()).thenReturn(31L);

        chunkList.append(chunk0);
        chunkList.append(chunk1);

        assertThat(chunkList.chunkIn(0L), is(chunk0));
        assertThat(chunkList.chunkIn(7L), is(chunk0));
        assertThat(chunkList.chunkIn(15L), is(chunk0));
        assertThat(chunkList.chunkIn(16L), is(chunk1));
        assertThat(chunkList.chunkIn(21L), is(chunk1));
        assertThat(chunkList.chunkIn(31L), is(chunk1));
    }
}
