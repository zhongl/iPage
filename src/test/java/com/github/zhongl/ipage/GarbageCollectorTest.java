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

import com.github.zhongl.buffer.CommonAccessors;
import com.github.zhongl.util.FileBase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class GarbageCollectorTest extends FileBase {

    @Test
    public void collect() throws Exception {
        file = testFile("collect");
        GarbageCollector<String> collector = new GarbageCollector<String>();
        Chunk<String> chunk = Chunk.appendableChunk(file, 0L, 4096, CommonAccessors.STRING);

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
}
