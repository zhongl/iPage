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

package com.github.zhongl.page;

import com.github.zhongl.builder.*;
import com.github.zhongl.util.FileHandler;
import com.github.zhongl.util.NumberNamedFilesLoader;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Binder<T> implements Closeable, Flushable {

    private final LinkedList<Page<T>> pages;

    Binder(File dir, int pageCapacity, Recorder<T> recorder) throws IOException {
        pages = tryLoadAndRecoverPages(dir, pageCapacity, recorder);
    }

    private LinkedList<Page<T>> tryLoadAndRecoverPages(File dir, final int pageCapacity, final Recorder<T> recorder) throws IOException {
        LinkedList<Page<T>> list = new NumberNamedFilesLoader<Page<T>>(dir, new FileHandler<Page<T>>() {
            @Override
            public Page<T> handle(File file, boolean last) throws IOException {
                return new Page<T>(file, last ? pageCapacity : (int) file.length(), recorder);
            }
        }).loadTo(new LinkedList<Page<T>>());

        if (list.isEmpty()) {
            list.addLast(new Page<T>(new File(dir, "0"), pageCapacity, recorder));
        } else {
            list.getLast().tryRecover();
        }
        return list;
    }

    public Cursor append(T record) throws OverflowException, IOException {
        try {
            return pages.getLast().append(record);
        } catch (IllegalStateException e) { // grow for retry one time
            pages.addLast(pages.getLast().multiply());
            return pages.getLast().append(record);
        }
    }

    public T get(Cursor cursor) throws UnderflowException, IOException {
        if (pages.getLast().compareTo(cursor) < 0) throw new UnderflowException();
        if (pages.getFirst().compareTo(cursor) > 0) return null; // non-existed cursor
        return pages.get(indexOf(cursor)).get(cursor);
    }

    public void remove(Cursor cursor) {
        // TODO remove
    }

    public Cursor next(Cursor cursor) {
        return pages.get(indexOf(cursor)).next(cursor);
    }

    private int indexOf(Cursor cursor) {
        int low = 0, high = pages.size() - 1;
        while (low <= high) { // binary search
            int mid = (low + high) >>> 1;
            int cmp = pages.get(mid).compareTo(cursor);
            if (cmp < 0) low = mid + 1;
            else if (cmp > 0) high = mid - 1;
            else return mid;
        }
        return -(low + 1);
    }

    @Override
    public void close() throws IOException {
        for (Page<T> page : pages) page.close();
    }

    @Override
    public void flush() throws IOException {
        pages.getLast().flush();
    }

    public static <T> Builder<T> baseOn(File dir) {
        return Builders.newInstanceOf(Builder.class).dir(dir);
    }

    public static interface Builder<T> extends BuilderConvention {

        @ArgumentIndex(0)
        @NotNull
        public Builder dir(File value);

        @ArgumentIndex(1)
        @DefaultValue("4096")
        @GreaterThanOrEqual("4096")
        public Builder pageCapacity(int value);

        @ArgumentIndex(2)
        @NotNull
        public Builder recorder(Recorder value);

        public Binder<T> build();
    }
}
