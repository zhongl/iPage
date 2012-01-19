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

package com.github.zhongl.journal1;

import com.github.zhongl.codec.Codec;
import com.github.zhongl.codec.StringCodec;
import com.github.zhongl.util.FileBase;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class JournalTest extends FileBase {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");


        int pageCapacity = 4906;
        Applicable<?> applicable = mock(Applicable.class);
        Codec codec = new StringCodec();
        Journal journal = new Journal(dir, pageCapacity, applicable, codec);

        byte[] bytes = "something".getBytes();

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        journal.append(Arrays.asList("1","2"), true);

        ByteBufferHandler handler = mock(ByteBufferHandler.class);

        journal.applyTo(handler);
        verify(handler, times(1)).handle(buffer);

        journal.applyTo(handler);
        verify(handler, times(1)).handle(buffer); // no buffer for applying

        journal.applyTo(handler);
        verify(handler, times(1)).handle(buffer); // ignore eof

        journal.close();
    }

    @Test
    public void loadOnePage() throws Exception {
        dir = testDir("loadOnePage");

        int capacity = 4096;
        Page page = new Page(new File(dir, "0"), capacity);

        ByteBuffer buffer0 = ByteBuffer.wrap("0".getBytes());
        page.append(buffer0);
        ByteBuffer buffer1 = ByteBuffer.wrap("1".getBytes());
        page.append(buffer1);

        Cursor head = page.head();
        page.remove();
        page.saveCheckpoint(head.position());
        page.close();

        Journal journal = Journal.open(dir, capacity);

        ByteBufferHandler handler = mock(ByteBufferHandler.class);
        journal.applyTo(handler);
        verify(handler, times(1)).handle(buffer1);

        journal.close();
    }

    @Test
    public void loadFullPageAndEmptyPage() throws Exception {
        dir = testDir("loadFullPageAndEmptyPage");

        int capacity = 45;
        Page page = new Page(new File(dir, "0"), capacity);

        ByteBuffer buffer0 = ByteBuffer.wrap("0".getBytes());
        page.append(buffer0);
        ByteBuffer buffer1 = ByteBuffer.wrap("1".getBytes());
        page.append(buffer1);

        Cursor head = page.head();
        page.remove();
        page.saveCheckpoint(head.position());

        page.close();

        new File(dir, "49").createNewFile();

        Journal journal = Journal.open(dir, capacity);

        ByteBufferHandler handler = mock(ByteBufferHandler.class);
        journal.applyTo(handler);
        verify(handler, times(1)).handle(buffer1);

        journal.close();
    }

}
