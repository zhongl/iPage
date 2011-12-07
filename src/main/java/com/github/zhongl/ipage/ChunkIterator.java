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

import com.github.zhongl.kvengine.Record;
import com.github.zhongl.util.FileNumberNameComparator;
import com.github.zhongl.util.NumberFileNameFilter;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class ChunkIterator extends AbstractIterator<Record> {

    private final Queue<Chunk> chunks = new LinkedList<Chunk>();

    private Iterator<Record> currentChunkIterator;
    private ByteBufferAccessor accessor;

    public ChunkIterator(File baseDir) throws IOException {
        checkArgument(baseDir.isDirectory(), "%s is not exist directory", baseDir);
        loadExistChunksFrom(baseDir);
    }

    private void loadExistChunksFrom(File baseDir) throws IOException {
        File[] files = baseDir.listFiles(new NumberFileNameFilter());
        Arrays.sort(files, new FileNumberNameComparator());

        for (File file : files) {
            Chunk chunk = new Chunk(Long.parseLong(file.getName()), file, file.length(), accessor);
            chunks.offer(chunk);
        }

        currentChunkIterator = nextChunkIterator();
    }

    @Override
    protected Record computeNext() {
        if (currentChunkIterator.hasNext()) return currentChunkIterator.next();
        // poll next one for iterate
        currentChunkIterator = nextChunkIterator();
        if (currentChunkIterator == null) return endOfData(); // no more iterator
        return currentChunkIterator.next();
    }

    private Iterator<Record> nextChunkIterator() {
        Chunk currentChunk = chunks.poll();
        if (currentChunk == null) return null;
        return new RecordIterator(currentChunk);
    }

    private static class RecordIterator extends AbstractIterator<Record> {
        private final Chunk chunk;
        private final Iterator<Record> iterator;

        public RecordIterator(Chunk chunk) {
            this.chunk = chunk;
            iterator = chunk.iterator();
        }

        @Override
        protected Record computeNext() {
            try {
                return iterator.next();
            } catch (NoSuchElementException e) {
                Closeables.closeQuietly(chunk);
                return endOfData();
            }
        }
    }
}
