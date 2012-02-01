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
import com.github.zhongl.util.FilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;

import static com.google.common.base.Preconditions.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Binder implements Closable {

    protected final File dir;
    protected final LinkedList<Page> pages;

    public Binder(File dir) throws IOException {
        this.dir = dir;
        this.pages = loadOrInitialize();
    }

    public <T> Cursor append(T value, boolean force) throws IOException {
        return pages.getLast().append(value, force, new OverflowCallback() {
            @Override
            public <T> Cursor onOverflow(T value, boolean force) throws IOException {
                Page page = newPage(pages.getLast());
                Cursor cursor = page.append(value, force, THROW_BY_OVERFLOW);
                pages.addLast(page);
                return cursor;
            }
        });
    }

    @Override
    public void close() {
        for (Page page : pages) page.close();
    }

    public <T> Cursor head() {
        return new Reader(pages.getFirst(), 0);
    }

    public <T> Cursor head(Number number) {
        int index = binarySearchPageIndex(number);
        checkArgument(index >= 0, "Too small number %s.", number);
        return new Reader(pages.get(index), 0);
    }

    public <T> Cursor next(Cursor cursor) {
        checkNotNull(cursor);
        Reader reader = Page.transform(cursor);
        int location = reader.offset + reader.length();

        if (location < reader.page.file().length())
            return new Reader(reader.page, location);

        int i = pages.indexOf(reader.page);
        if (i + 1 == pages.size()) return null;
        return new Reader(pages.get(i + 1), 0);
    }

    public void reset() {
        while (!pages.isEmpty()) removePages();
        pages.add(newPage(null));
    }

    public void removePagesFromHeadTo(Number number) {
        int i = binarySearchPageIndex(number);
        for (int j = 0; j < i; j++) removePages();
    }

    private void removePages() {
        Page page = pages.remove();
        page.close();
        checkState(page.file().delete());
    }

    /**
     * @param number
     *
     * @return exist max page number closed the input number.
     */
    public Number roundPageNumber(Number number) {
        int index = binarySearchPageIndex(number);
        checkArgument(index >= 0, "Too small number %s.", number);
        return pages.get(index).number();
    }

    private LinkedList<Page> loadOrInitialize() throws IOException {
        LinkedList<Page> list = new FilesLoader<Page>(
                dir,
                new FilterAndComparator() {
                    @Override
                    public int compare(File o1, File o2) {
                        return parseNumber(o1.getName()).compareTo(parseNumber(o2.getName()));
                    }

                    @Override
                    public boolean accept(File dir, String name) {
                        try {
                            parseNumber(name);
                            return true;
                        } catch (RuntimeException e) {
                            return false;
                        }
                    }
                },
                new Transformer<Page>() {
                    @Override
                    public Page transform(File file, boolean last) throws IOException {
                        return newPage(file, parseNumber(file.getName()));
                    }
                }
        ).loadTo(new LinkedList<Page>());

        if (list.isEmpty()) list.add(newPage(null));
        return list;
    }

    private Page newPage(Page last) {
        Number number = newNumber(last);
        return newPage(new File(dir, number.toString()), number);
    }

    private int binarySearchPageIndex(Number number) {
        int i = Collections.binarySearch(pages, new Numbered(number) {});
        return -(i + 2);
    }

    protected abstract Page newPage(File file, Number number);

    protected abstract Number newNumber(@Nullable Page last);

    protected abstract Number parseNumber(String text);
}
