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

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.FilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Binder implements Closable, Appendable {

    protected final File dir;
    protected final Codec codec;
    protected final List<Page> pages;

    protected Binder(File dir, Codec codec) throws IOException {
        this.dir = dir;
        this.codec = codec;
        this.pages = loadOrInitialize();
    }

    @Override
    public <T> Cursor append(T value, boolean force) throws IOException {
        return last().append(value, force, new OverflowCallback() {
            @Override
            public <T> Cursor onOverflow(T value, boolean force) throws IOException {
                Page page = newPage(last());
                Cursor cursor = page.append(value, force, THROW_BY_OVERFLOW);
                pages.add(page);
                return cursor;
            }
        });
    }

    @Override
    public void close() {
        for (Page page : pages) page.close();
    }

    protected void removeHeadPage() {
        Page page = pages.remove(0);
        page.close();
        checkState(page.file().delete());
    }

    protected Page last() {return pages.get(pages.size() - 1);}

    protected Page newPage(Page last) {
        Number number = newNumber(last);
        return newPage(new File(dir, number.toString()), number, codec);
    }

    protected int binarySearchPageIndex(Number number) {
        int i = Collections.binarySearch(pages, new Numbered(number) {});
        return i < 0 ? -(i + 2) : i;
    }

    protected abstract Page newPage(File file, Number number, Codec codec);

    protected abstract Number newNumber(@Nullable Page last);

    protected abstract Number parseNumber(String text);

    private List<Page> loadOrInitialize() throws IOException {
        List<Page> list = new FilesLoader<Page>(
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
                        } catch (RuntimeException e) { // Invalid format Error
                            return false;
                        }
                    }
                },
                new Transformer<Page>() {
                    @Override
                    public Page transform(File file, boolean last) throws IOException {
                        return newPage(file, parseNumber(file.getName()), codec);
                    }
                }
        ).loadTo(new ArrayList<Page>());

        if (list.isEmpty()) list.add(newPage(null));
        return list;
    }
}
