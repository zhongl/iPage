/*
 * Copyright 2012 zhongl
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
import com.github.zhongl.page.LengthReader;
import com.github.zhongl.page.LengthWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class EventAccessor implements Accessor<Event> {

    @Override
    public Writer writer(Event value) {
        final StringEvent event = (StringEvent) value;
        return new LengthWriter() {
            @Override
            public int bodyByteLength() {
                return event.value.length();
            }

            protected int writeBodyTo(WritableByteChannel channel) throws IOException {
                return channel.write(ByteBuffer.wrap(event.value.getBytes()));
            }
        };
    }

    @Override
    public Reader<Event> reader() {
        return new LengthReader<Event>() {

            protected Event readBodyFrom(ReadableByteChannel channel, int length) throws IOException {
                byte[] bytes = new byte[length];
                channel.read(ByteBuffer.wrap(bytes));
                return new StringEvent(new String(bytes));
            }
        };
    }
}
