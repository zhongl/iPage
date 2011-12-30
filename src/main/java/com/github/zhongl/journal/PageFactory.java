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

package com.github.zhongl.journal;

import com.github.zhongl.util.FileHandler;
import com.github.zhongl.util.NumberNamedFilesLoader;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class PageFactory {
    private final File dir;
    private final Accessor<Event> accessor;

    private long lastIndex;
    private final List<Page> pages;

    public PageFactory(File dir, Accessor<Event> accessor) throws IOException {
        this.dir = dir;
        this.accessor = accessor;
        pages = loadUnappliedPages();
    }

    public Page create() throws IOException {
        return new Page(new File(dir, Long.toString(lastIndex++)), accessor);
    }

    public List<Page> unappliedPages() {
        return pages;
    }

    private List<Page> loadUnappliedPages() throws IOException {
        return new NumberNamedFilesLoader<Page>(dir, new FileHandler<Page>() {
            @Override
            public Page handle(File file, boolean last) throws IOException {
                try {
                    Page page = new Page(file, accessor);
                    lastIndex = Long.parseLong(file.getName()) + 1;
                    return page;
                } catch (IllegalStateException e) {
                    file.delete();
                    return null;
                }
            }
        }).loadTo(new LinkedList<Page>());
    }
}
