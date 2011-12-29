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

import com.github.zhongl.durable.File;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Page implements Iterable<Event> {

    private final File file;
    private final List<Event> list;

    public Page(File file) {
        this.file = file;
        list = new LinkedList<Event>();
    }

    public void add(Event event) throws IOException {
        list.add(event);
        file.writeFully(event.toByteBuffer());
    }

    public void fix() throws IOException {
        file.fix();
    }

    @Override
    public Iterator<Event> iterator() {
        return list.iterator();
    }

    public void clear() {
        file.delete();
    }
}
