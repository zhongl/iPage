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

    @Deprecated
    public long collect(long survivorOffset) throws IOException {
        if (lastSurvivorOffset == survivorOffset || chunkList.isEmpty()) return 0L;

        long beginPosition = chunkList.first().beginPosition();

        boolean firstCollection = lastSurvivorOffset < beginPosition;
        boolean recollectFromStart = lastSurvivorOffset > survivorOffset;

        if (firstCollection || recollectFromStart) lastSurvivorOffset = beginPosition;

        long collectedLength = collect(lastSurvivorOffset, survivorOffset);
        lastSurvivorOffset = survivorOffset;
        return collectedLength;
    }

    public long collect(long begin, long end) throws IOException {
        if (begin == end || chunkList.isEmpty() || end - begin < minimizeCollectLength) return 0L;

        int indexOfBeginChunk = chunkList.indexOfChunkIn(begin);
        int indexOfEndChunk = chunkList.indexOfChunkIn(end);

        indexOfBeginChunk = indexOfBeginChunk < 0 ? 0 : indexOfBeginChunk;
        indexOfEndChunk = indexOfEndChunk < 0 ? chunkList.lastIndex() : indexOfEndChunk;

        if (indexOfBeginChunk == indexOfEndChunk) return collectIn(indexOfBeginChunk, begin, end);

        /*
         *   |   left    |  between |   right   |
         *   |@@@@@|-----|----------|-----|@@@@@|
         *   |      0    |    1     |     2     |
         */
        return collectRight(indexOfEndChunk, end) + collectBetween(indexOfEndChunk, indexOfBeginChunk) + collectLeft(indexOfBeginChunk, begin);
    }

    /** @see Chunk#right(long) */
    private long collectRight(int index, long offset) throws IOException {
        Chunk<T> right = chunkList.get(index);
        Chunk<T> newChunk = right.right(offset);
        if (newChunk == right) return 0L;
        chunkList.set(index, newChunk);
        return offset - right.beginPosition();
    }

    /** @see Chunk#left(long) */
    private long collectLeft(int index, long offset) throws IOException {
        Chunk<T> left = chunkList.get(index);
        long collectedLength = left.endPosition() + 1 - offset;
        Chunk<T> newLeft = left.left(offset);
        if (newLeft == null) chunkList.remove(index);
        else chunkList.set(index, newLeft);
        return collectedLength;
    }

    private long collectBetween(int indexOfEndChunk, int indexOfBeginChunk) {
        long collectedLength = 0L;
        for (int i = indexOfEndChunk - 1; i > indexOfBeginChunk; i--) {
            Chunk<T> chunk = chunkList.remove(i);
            collectedLength += chunk.length();
            chunk.delete();
        }
        return collectedLength;
    }

    /** @see Chunk#split(long, long) */
    private long collectIn(int indexOfChunk, long begin, long end) throws IOException {
        Chunk<T> splittingChunk = chunkList.get(indexOfChunk);
        List<? extends Chunk<T>> pieces = splittingChunk.split(begin, end);
        if (pieces.get(0).equals(splittingChunk)) return 0L; // can't left appending chunk
        chunkList.set(indexOfChunk, pieces.get(0));
        chunkList.insert(indexOfChunk + 1, pieces.get(1));
        return end - begin;
    }


}
