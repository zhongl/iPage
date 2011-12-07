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

import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class ChunkIterator<T> extends AbstractIterator<T> {

    private final Queue<Chunk<T>> chunks;

    private Iterator<T> currentChunkIterator;

    public ChunkIterator(Queue<Chunk<T>> chunks) {
        this.chunks = chunks;
        currentChunkIterator = nextChunkIterator();
    }

    @Override
    protected T computeNext() {
        if (currentChunkIterator.hasNext()) return currentChunkIterator.next();
        // poll next one for iterate
        currentChunkIterator = nextChunkIterator();
        if (currentChunkIterator == null) return endOfData(); // no more iterator
        return currentChunkIterator.next();
    }

    private Iterator<T> nextChunkIterator() {
        Chunk currentChunk = chunks.poll();
        if (currentChunk == null) return null;
        return new RecordIterator<T>(currentChunk);
    }

    private static class RecordIterator<T> extends AbstractIterator<T> {
        private final Chunk chunk;
        private final Iterator<T> iterator;

        public RecordIterator(Chunk chunk) {
            this.chunk = chunk;
            iterator = chunk.iterator();
        }

        @Override
        protected T computeNext() {
            try {
                return iterator.next();
            } catch (NoSuchElementException e) {
                Closeables.closeQuietly(chunk);
                return endOfData();
            }
        }
    }
}
