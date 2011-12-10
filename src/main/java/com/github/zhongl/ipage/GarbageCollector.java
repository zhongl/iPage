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

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class GarbageCollector<T> {
    private volatile long lastSurvivorOffset = -1L;

    public long collect(long survivorOffset, ChunkList<T> chunkList) throws IOException {
        if (lastSurvivorOffset == survivorOffset) return 0L;

        long beginPosition = chunkList.first().beginPosition();

        boolean firstCollection = lastSurvivorOffset < beginPosition;
        boolean recollectFromStart = lastSurvivorOffset > survivorOffset;

        if (firstCollection || recollectFromStart) lastSurvivorOffset = beginPosition;

        long collectedLength = chunkList.garbageCollect(lastSurvivorOffset, survivorOffset);
        lastSurvivorOffset = survivorOffset;
        return collectedLength;
    }

}
