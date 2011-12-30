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

import com.github.zhongl.page.Accessor;
import com.github.zhongl.page.Page;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class EventPage extends Page<Event> {

    private final List<Event> list;

    EventPage(File file, Accessor<Event> accessor) throws IOException {
        super(file, accessor);
        this.list = tryLoadFromExistFile();
        if (!list.isEmpty()) fix();
    }

    @Override
    public int add(Event object) throws IOException {
        list.add(object);
        return super.add(object);
    }

    @Override
    public void clear() {
        list.clear();
        super.clear();
    }

    @Override
    public Iterator<Event> iterator() {
        return list.iterator();
    }

    protected WritableByteChannel writeChannel() {return writeOnlychannel;}

    private List<Event> tryLoadFromExistFile() throws IOException {
        if (!file.exists()) return new LinkedList<Event>();
        FileInputStream stream = new FileInputStream(file);
        try {
            return load(stream, file.length() - CRC32_LENGTH);
        } finally {
            stream.close();
        }
    }

    private List<Event> load(FileInputStream stream, long offset) throws IOException {
        FileChannel channel = stream.getChannel();
        LinkedList<Event> events = new LinkedList<Event>();
        channel.position(0L);
        while (channel.position() < offset) {
            events.add(accessor.reader().readFrom(channel));
        }
        return events;
    }
}
