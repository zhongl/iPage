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

package com.github.zhongl.sequence;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Sequence<T> implements Closeable {

    private final LinkedList<LinkedPage<T>> linkedPages;

    public Sequence(LinkedList<LinkedPage<T>> linkedPages) {
        this.linkedPages = linkedPages;
    }

    public Cursor append(T object) throws OverflowException, IOException {
        try {
            return linkedPages.getLast().append(object);
        } catch (IllegalStateException e) { // grow for retry one time
            linkedPages.addLast(linkedPages.getLast().multiply());
            return linkedPages.getLast().append(object);
        }
    }

    public T get(Cursor cursor) throws UnderflowException, IOException {
        if (linkedPages.getLast().compareTo(cursor) < 0) throw new UnderflowException();
        if (linkedPages.getFirst().compareTo(cursor) > 0) return null; // non-existed cursor
        return linkedPages.get(indexOf(cursor)).get(cursor);
    }

    public Cursor next(Cursor cursor) throws IOException {
        return linkedPages.get(indexOf(cursor)).next(cursor);
    }

    private int indexOf(Cursor cursor) {
        int low = 0, high = linkedPages.size() - 1;
        while (low <= high) { // binary search
            int mid = (low + high) >>> 1;
            int cmp = linkedPages.get(mid).compareTo(cursor);
            if (cmp < 0) low = mid + 1;
            else if (cmp > 0) high = mid - 1;
            else return mid;
        }
        return -(low + 1);
    }

    @Override
    public void close() throws IOException {
        for (LinkedPage<T> linkedPage : linkedPages) linkedPage.close();
    }

}
