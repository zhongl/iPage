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

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkBase extends FileBase {
    protected Chunk<String> chunk;

    @Override
    public void tearDown() throws Exception {
        chunk.close();
        super.tearDown();
    }

    protected void newChunk() throws IOException {
        long beginPositionInIPage = 0L;
        int capacity = 4096;
        int minimizeCollectLength = 16;
        chunk = new Chunk(file, capacity, beginPositionInIPage, minimizeCollectLength, CommonAccessors.STRING);
    }
}
