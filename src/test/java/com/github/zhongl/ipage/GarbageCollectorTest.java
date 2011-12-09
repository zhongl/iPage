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
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class GarbageCollectorTest extends FileBase {

    Builder.ChunkFactory<String> chunkFactory = new Builder.ChunkFactory<String>(4096L, CommonAccessors.STRING);

    @Test
    @Ignore
    public void collect() throws Exception {
        dir = testDir("collect");
        dir.mkdirs();
        GarbageCollector<String> collector = new GarbageCollector<String>();
        ChunkList<String> chunkList = new ChunkList<String>(dir, chunkFactory);
        Chunk<String> chunk = chunkList.last();
        for (int i = 0; i < 10; i++) {
            chunk.append("0123456789ab");
        }
        assertExistFile("0");

        assertThat(collector.collect(64L, chunkList), is(64L));

        assertNotExistFile("0");
        assertExistFile("64");
    }
}
