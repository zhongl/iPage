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

import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.CRC32;

import static com.github.zhongl.util.FileAsserter.assertExist;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileBase {

    @Test
    public void main() throws Exception {
        file = testFile("main");

        Page page = new Page(file, new EventChannelAccessor());

        Event event = new StringEvent("event");

        page.add(event);

        page.fix();

        byte[] content = Bytes.concat(Ints.toByteArray(5), "event".getBytes());
        CRC32 crc32 = new CRC32();
        crc32.update(content);

        assertExist(file).contentIs(content, Longs.toByteArray(crc32.getValue()));

        assertThat(page.iterator().next(), is(event));

        try {
            page.add(new StringEvent(""));
            fail("Should throw exception");
        } catch (IllegalStateException e) {
        }

        page.clear();

        assertThat(file.exists(), is(false));
    }

    @Test
    public void loadExist() throws Exception {
        file = testFile("loadExist");
        byte[] content = Bytes.concat(Ints.toByteArray(5), "event".getBytes());
        CRC32 crc32 = new CRC32();
        crc32.update(content);
        byte[] crc32Bytes = Longs.toByteArray(crc32.getValue());
        Files.write(Bytes.concat(content, crc32Bytes), file);

        Page page = new Page(file, new EventChannelAccessor());
        StringEvent event = (StringEvent) page.iterator().next();
        assertThat(event.value, is("event"));

        try {
            page.add(new StringEvent(""));
            fail("Should throw exception");
        } catch (IllegalStateException e) {
        }
    }

    @Test(expected = IllegalStateException.class)
    public void loadInvalidExist() throws Exception {
        file = testFile("loadInvalidExist");
        Files.write(Bytes.concat(Ints.toByteArray(5), "event".getBytes(), Longs.toByteArray(4L)), file);
        new Page(file, new EventChannelAccessor());
    }

    private static class StringEvent implements Event {

        private final String value;

        public StringEvent(String value) {
            this.value = value;
        }

        @Override
        public void onCommit() { }

        @Override
        public void onError(Throwable t) { }

    }

    private static class EventChannelAccessor implements ChannelAccessor<Event> {
        @Override
        public Writer writer(Event value) {
            final StringEvent event = (StringEvent) value;
            return new Writer() {
                @Override
                public int valueByteLength() {
                    return event.value.length();
                }

                @Override
                public int writeTo(WritableByteChannel channel) throws IOException {
                    return channel.write(ByteBuffer.wrap(event.value.getBytes()));
                }
            };
        }

        @Override
        public Reader<Event> reader(final int length) {
            return new Reader<Event>() {
                @Override
                public Event readFrom(ReadableByteChannel channel) throws IOException {
                    byte[] bytes = new byte[length];
                    channel.read(ByteBuffer.wrap(bytes));
                    return new StringEvent(new String(bytes));
                }
            };
        }
    }
}
