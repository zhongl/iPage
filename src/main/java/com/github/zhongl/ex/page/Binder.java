/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ex.page;

import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Collections;
import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Binder implements Closable {

    private final File dir;
    private final LinkedList<Page> pages;

    public Binder(File dir) throws IOException {
        this.dir = dir;
        this.pages = loadOrInitialize();
    }

    public <T> Cursor<T> append(T value, boolean force) throws IOException {
        return pages.getLast().append(value, force, new OverflowCallback<T>() {
            @Override
            public Cursor<T> onOverflow(T value, boolean force) throws IOException {
                Page page = newPage(pages.getLast());
                Cursor<T> cursor = page.append(value, force, THROW_BY_OVERFLOW);
                pages.addLast(page);
                return cursor;
            }
        });
    }

    @Override
    public void close() {
        for (Page page : pages) page.close();
    }

    private LinkedList<Page> loadOrInitialize() throws IOException {
        LinkedList<Page> list = new FilesLoader<Page>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<Page>() {
                    @Override
                    public Page transform(File file, boolean last) throws IOException {
                        return newPage(file, Long.parseLong(file.getName()));
                    }
                }).loadTo(new LinkedList<Page>());

        if (list.isEmpty()) list.add(newPage(null));
        return list;
    }

    private Page newPage(Page last) {
        long number = newPageNumber(last);
        return newPage(new File(dir, number + ""), number);
    }

    public <T> Cursor<T> head() {
        return new Reader<T>(pages.getFirst(), 0);
    }

    public <T> Cursor<T> next(Cursor<?> cursor) {
        checkNotNull(cursor);
        Reader<?> reader = Page.transform(cursor);
        long location = reader.offset + reader.length();
        Page page = binarySearch(location);
        if (page == null) return null;
        return new Reader<T>(page, (int) (location - page.number()));
    }

    private Page binarySearch(long location) {
        int i = Collections.binarySearch(new AbstractList<Long>() {

            @Override
            public Long get(int index) {
                return pages.get(index).number();
            }

            @Override
            public int size() {
                return pages.size();
            }
        }, location);

        i = -(i + 2);
        if (i < 0) return null;

        Page page = pages.get(i);
        if (location >= page.number() + page.file().length()) return null;
        return page;
    }

    protected abstract Page newPage(File file, long number);

    protected abstract long newPageNumber(@Nullable Page last);
}
