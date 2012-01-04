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

package com.github.zhongl.sequence;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Sequence<T> implements Closeable {

    private final LinkedList<LinkedPage<T>> linkedPages;
    private final GarbageCollector<T> garbageCollector;

    public Sequence(SequenceLoader<T> loader, long minimizeCollectLength) throws IOException {
        this.linkedPages = loader.load();
        garbageCollector = new GarbageCollector<T>(linkedPages, minimizeCollectLength);
    }

    public Cursor append(T object) throws OverflowException, IOException {
        return linkedPages.getLast().append(object);
    }

    public void addNewPage() throws IOException {
        linkedPages.addLast(linkedPages.getLast().multiply());
    }

    public Cursor tail() {
        LinkedPage<T> last = linkedPages.getLast();
        return new Cursor(last.begin() + last.length());
    }

    public void fixLastPage() throws IOException {
        linkedPages.getLast().fix();
    }

    public T get(Cursor cursor) throws UnderflowException, IOException {
        if (linkedPages.getLast().compareTo(cursor) < 0) throw new UnderflowException();
        if (linkedPages.getFirst().compareTo(cursor) > 0) return null; // non-existed cursor
        int index = cursor.indexIn(linkedPages);
        return linkedPages.get(index).get(cursor);
    }

    public Cursor next(Cursor cursor) throws IOException {
        int index = cursor.indexIn(linkedPages);
        return linkedPages.get(index).next(cursor);
    }

    @Override
    public void close() throws IOException {
        for (LinkedPage<T> linkedPage : linkedPages) linkedPage.close();
    }

    public long collect(Cursor begin, Cursor end) throws IOException {
        return garbageCollector.collect(begin, end);
    }

}
