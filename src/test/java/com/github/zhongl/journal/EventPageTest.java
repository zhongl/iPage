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

import com.github.zhongl.cache.Cache;
import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.zip.CRC32;

import static com.github.zhongl.util.FileAsserter.assertExist;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class EventPageTest extends FileTestContext {


    private Cache cache;

    @Before
    public void setUp() throws Exception {
        cache = mock(Cache.class);
    }

    @Test
    public void main() throws Exception {
        dir = testDir("main");
        file = new File(dir, "0");

        EventPage eventPage = new EventPage(file, new EventAccessor(), cache);

        Event event = new StringEvent("event");

        eventPage.add(event);

        eventPage.fix();

        byte[] content = Bytes.concat(Ints.toByteArray(5), "event".getBytes());
        CRC32 crc32 = new CRC32();
        crc32.update(content);

        assertExist(file).contentIs(content, Longs.toByteArray(crc32.getValue()));

        assertThat(eventPage.iterator().next(), is(event));

        try {
            eventPage.add(new StringEvent(""));
            fail("Should throw exception");
        } catch (IllegalStateException e) {
        }

        eventPage.clear();

        verify(cache).weak(event);

        assertThat(file.exists(), is(false));

        assertThat(eventPage.iterator().hasNext(), is(false));
    }

    @Test
    public void loadExist() throws Exception {
        dir = testDir("loadExist");
        file = new File(dir, "0");

        byte[] content = Bytes.concat(Ints.toByteArray(5), "event".getBytes());
        CRC32 crc32 = new CRC32();
        crc32.update(content);
        byte[] crc32Bytes = Longs.toByteArray(crc32.getValue());
        Files.write(Bytes.concat(content, crc32Bytes), file);

        EventPage eventPage = new EventPage(file, new EventAccessor(), cache);
        StringEvent event = (StringEvent) eventPage.iterator().next();
        assertThat(event.value, is("event"));

        try {
            eventPage.add(new StringEvent(""));
            fail("Should throw exception");
        } catch (IllegalStateException e) {
        }
    }

    @Test(expected = IllegalStateException.class)
    public void loadInvalidExist() throws Exception {
        dir = testDir("loadInvalidExist");
        file = new File(dir, "0");

        Files.write(Bytes.concat(Ints.toByteArray(5), "event".getBytes(), Longs.toByteArray(4L)), file);
        new EventPage(file, new EventAccessor(), cache);
    }

}
