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
import org.junit.Test;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileBase {


    @Test
    public void addAndFix() throws Exception {
        file = testFile("addAndFix");

        FileChannel fileChannel = (FileChannel) spy(Channels.newChannel(new FileOutputStream(file)));

        Page page = new Page(fileChannel);

        Event event = mock(Event.class);
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        doReturn(byteBuffer).when(event).toByteBuffer();

        page.add(event);

        verify(fileChannel).write(byteBuffer);

        page.fix();

        verify(fileChannel).close();
    }

}
