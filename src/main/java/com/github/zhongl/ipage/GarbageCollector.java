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
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class GarbageCollector<T> {
    private final long minimizeCollectLength;
    private final ChunkList<T> chunkList;

    private long lastSurvivorOffset = -1L;

    public GarbageCollector(ChunkList<T> chunkList, long minimizeCollectLength) {
        this.chunkList = chunkList;
        this.minimizeCollectLength = minimizeCollectLength;
    }

    public long collect(long survivorOffset) throws IOException {
        if (lastSurvivorOffset == survivorOffset) return 0L;

        long beginPosition = chunkList.first().beginPosition();

        boolean firstCollection = lastSurvivorOffset < beginPosition;
        boolean recollectFromStart = lastSurvivorOffset > survivorOffset;

        if (firstCollection || recollectFromStart) lastSurvivorOffset = beginPosition;

        long collectedLength = collect(lastSurvivorOffset, survivorOffset);
        lastSurvivorOffset = survivorOffset;
        return collectedLength;
    }

    private long collect(long begin, long end) throws IOException {
        if (end - begin < minimizeCollectLength) return 0L;
        int indexOfBeginChunk = chunkList.indexOfChunkIn(begin);
        int indexOfEndChunk = chunkList.indexOfChunkIn(end);
        if (indexOfBeginChunk == indexOfEndChunk) return collectIn(indexOfBeginChunk, begin, end);
        return collectRight(indexOfEndChunk, end) + collectLeft(indexOfBeginChunk, begin);
    }

    /** @see Chunk#right(long) */
    private long collectRight(int indexOfEndChunk, long end) throws IOException {
        Chunk<T> right = chunkList.get(indexOfEndChunk);
        Chunk<T> newChunk = right.right(end);
        if (newChunk == right) return 0L;
        chunkList.set(indexOfEndChunk, newChunk);
        return end - right.beginPosition();
    }

    /** @see Chunk#left(long) */
    private long collectLeft(int indexOfBeginChunk, long begin) throws IOException {
        Chunk<T> left = chunkList.get(indexOfBeginChunk);
        long collectedLength = left.endPosition() + 1 - begin;
        Chunk<T> newLeft = left.left(begin);
        if (newLeft == null) chunkList.remove(indexOfBeginChunk);
        else chunkList.set(indexOfBeginChunk, newLeft);
        return collectedLength;
    }

    /** @see Chunk#split(long, long) */
    private long collectIn(int indexOfChunk, long begin, long end) throws IOException {
        Chunk<T> splittingChunk = chunkList.get(indexOfChunk);
        List<? extends Chunk<T>> pieces = splittingChunk.split(begin, end);
        if (pieces.get(0).equals(splittingChunk)) return 0L; // can't left appending chunk
        chunkList.set(indexOfChunk, pieces.get(0));
        if (pieces.size() == 2) chunkList.insert(indexOfChunk + 1, pieces.get(1));
        return end - begin;
    }


}
