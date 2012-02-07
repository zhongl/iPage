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
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Binder<V> implements Closable, Appendable<V> {

    protected final File dir;
    protected final Codec codec;
    protected final List<Page<V>> pages;

    protected Binder(File dir, Codec codec) throws IOException {
        this.dir = dir;
        this.codec = codec;
        this.pages = loadOrInitialize();
    }

    @Override
    public void append(V value, FutureCallback<Cursor> callback) {
        boolean append = last().append(value, callback);
        if (!append) { // overflow
            last().force();
            Page page = newPage(last());
            append = page.append(value, callback);
            checkState(append, "Too big value to append.");
            pages.add(page);
        }
    }

    @Override
    public void force() { last().force(); }

    @Override
    public void close() { for (Page page : pages) page.close(); }

    protected Page<V> newPage(Page last) {
        Number number = newNumber(last);
        return newPage(new File(dir, number.toString()), number, codec);
    }

    protected int binarySearchPageIndex(Number number) {
        int i = Collections.binarySearch(pages, new Numbered(number) {});
        return i < 0 ? -(i + 2) : i;
    }

    protected abstract Number newNumber(@Nullable Page last);

    protected abstract Number parseNumber(String text);

    protected abstract Page<V> newPage(File file, Number number, Codec codec);

    private Page<V> last() {return pages.get(pages.size() - 1);}

    private List<Page<V>> loadOrInitialize() throws IOException {
        List<Page<V>> list = new FilesLoader<Page<V>>(
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
                new Transformer<Page<V>>() {
                    @Override
                    public Page<V> transform(File file, boolean last) throws IOException {
                        return newPage(file, parseNumber(file.getName()), codec);
                    }
                }
        ).loadTo(new ArrayList<Page<V>>());

        if (list.isEmpty()) list.add(newPage(null));
        return list;
    }

}
