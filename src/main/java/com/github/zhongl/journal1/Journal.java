/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.journal1;


import com.github.zhongl.codec.Codec;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closeable {

    private volatile Page first;
    private volatile Page last;
    private Applicable<?> applicable;

    public Journal(Page first, Page last) {
        this.first = first;
        this.last = last;
    }

    public Journal(Page page) {
        this(page, page);
    }

    public Journal(File dir, Applicable<?> applicable, Codec... compoundCodecs) {
        this.applicable = applicable;


    }

    public static Journal open(File dir, final int pageCapacity) throws IOException {
        LinkedList<Page> pages = new FilesLoader<Page>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<Page>() {

                    @Override
                    public Page transform(File file, boolean last) throws IOException {
                        return new Page(file, pageCapacity);
                    }
                }).loadTo(new LinkedList<Page>());

        if (pages.isEmpty()) return new Journal(new Page(new File(dir, "0"), pageCapacity));

        long lastCheckpoint = lastCheckpointOf(pages);

        Page first = pages.get(pageIndex(lastCheckpoint, pages));
        first.setHead(lastCheckpoint);
        first.remove();
        return new Journal(first, pages.getLast());
    }

    private static long lastCheckpointOf(LinkedList<Page> pages) throws IOException {
        long lastCheckpoint = 0L;
        for (int i = pages.size() - 1; i >= 0; i--) {
            Page page = pages.get(i);
            long checkpoint = page.recoverAndGetLastCheckpoint();
            if (checkpoint >= 0) {
                lastCheckpoint = checkpoint;
                break;
            }
        }
        return lastCheckpoint;
    }

    public void append(ByteBuffer buffer) throws IOException {
        last = last.append(buffer);
    }

    @Override
    public void close() throws IOException {
        first.close();
        last.close();
    }

    public void applyTo(ByteBufferHandler handler) throws Exception {
        try {
            Cursor head = first.head();
            handler.handle(head.get());
            first = first.remove();
            if (head.get().limit() > 0)
                last = last.saveCheckpoint(head.position());
        } catch (EOFException ignored) { }
    }

    private static int pageIndex(long position, LinkedList<Page> pages) {
        int low = 0, high = pages.size() - 1;
        while (low <= high) { // binary search
            int mid = (low + high) >>> 1;

            int cmp = pages.get(mid).compareTo(position);
            if (cmp < 0) low = mid + 1;
            else if (cmp > 0) high = mid - 1;
            else return mid;
        }
        return -(low + 1);
    }

    public void append(Object event, boolean force) {

        // TODO append
    }
}
