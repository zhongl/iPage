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
import static com.google.common.base.Preconditions.checkState;

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

    public <T> Cursor<T> head(long number) {
        return new Reader<T>(pages.get(binarySearchPageIndex(number)), 0);
    }

    public <T> Cursor<T> next(Cursor<?> cursor) {
        checkNotNull(cursor);
        Reader<?> reader = Page.transform(cursor);
        int location = reader.offset + reader.length();

        if (location < reader.page.file().length())
            return new Reader<T>(reader.page, location);

        int i = pages.indexOf(reader.page);
        if (i + 1 == pages.size()) return null;
        return new Reader<T>(pages.get(i + 1), 0);
    }

    public void reset() {
        while (!pages.isEmpty()) removePages();
        pages.add(newPage(null));
    }

    public void removePagesFromHeadTo(long number) {
        int i = binarySearchPageIndex(number);
        for (int j = 0; j < i; j++) removePages();
    }

    private void removePages() {
        Page page = pages.remove();
        page.close();
        checkState(page.file().delete());
    }

    public long roundPageNumber(long number) {
        return pages.get(binarySearchPageIndex(number)).number();
    }

    private int binarySearchPageIndex(long number) {
        int i = Collections.binarySearch(new AbstractList<Long>() {

            @Override
            public Long get(int index) {
                return pages.get(index).number();
            }

            @Override
            public int size() {
                return pages.size();
            }
        }, number);

        return -(i + 2);
    }


    protected abstract Page newPage(File file, long number);

    protected abstract long newPageNumber(@Nullable Page last);
}
