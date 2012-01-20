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

package com.github.zhongl.journal1;

import com.github.zhongl.codec.Codec;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Pages implements Closable, Iterable<Record> {

    private final LinkedList<Page> pages;

    public Pages(File dir, Codec codec, int pageCapacity) {
        pages = loadOrInitialize(dir, codec, pageCapacity);
    }

    private LinkedList<Page> loadOrInitialize(File dir, Codec codec, int pageCapacity) {
        LinkedList<Page> list = new LinkedList<Page>();
        checkArgument(dir.list().length == 0, "TODO load exist pages.");

        if (list.isEmpty()) list.add(newPage(dir, codec, pageCapacity, 0L)); // initialize

        return list;  // TODO loadOrInitialize
    }

    private Page newPage(File dir, Codec codec, int pageCapacity, long number) {
        return null;  // TODO newPage
    }

    /**
     * Append any object which the {@link com.github.zhongl.codec.Codec} supported.
     *
     * @param object
     * @param force
     *
     * @return appended position.
     */
    public Record append(final Object object, final boolean force) {
        return pages.getLast().append(object, force, new OverflowCallback() {
            @Override
            public void onOverflow() {
                Page last = pages.getLast();
                Record tail = last.range().tail();
                Page newPage = last.newPage(tail.offset() + tail.length());
                newPage.append(object, force, new OverflowCallback() {
                    @Override
                    public void onOverflow() {
                        throw new IllegalStateException("Object is too big to append to new page.");
                    }
                });
                pages.addLast(newPage);
            }
        });
    }

    public void reset() {
        Page newPage = pages.getLast().newPage(0L);
        for (Page page : pages) page.delete();
        pages.clear();
        pages.addLast(newPage);
    }

    public void trimBefore(long position) {
        for (Page page : pages) {
            if(page.range().compareTo(position) < 0) page.delete();
        }

        // TODO trimBefore
    }

    public void trimAfter(long position) {
        // TODO trimAfter
    }

    @Override
    public void close() {
        for (Page page : pages) {
            page.close();
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return null;  // TODO iterator
    }

}
