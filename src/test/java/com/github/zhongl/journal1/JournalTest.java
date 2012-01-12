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

import com.github.zhongl.util.FileBase;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class JournalTest extends FileBase {

    @Test
    public void normal() throws Exception {
        dir = testDir("normal");

        Journal journal = Journal.open(dir);

        byte[] bytes = "something".getBytes();

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        journal.append(buffer);

        ByteBufferHandler handler = mock(ByteBufferHandler.class);

        journal.applyTo(handler);
        verify(handler, times(1)).handle(buffer);

        journal.applyTo(handler);
        verify(handler, times(1)).handle(buffer); // no buffer for applying

        journal.close();
    }

    /*

    Event : {CRC32, Type, length, boby}

    journal : {[Event|LastHead]...}


    {EEEEELEEEELLLLL}

    Directory {
        0, 1*PAGE_SIZE, 2*PAGE_SIZE, ..., n*PAGE_SIZE

        |---------|---------|---------|---------|

    }

    class Journal(Directory, EventAccessor){
        Cursor head
    }

    journal.append(event) {
        tailPage = tailPage.append(event)
    }

    book.append(event)

    page.append(event) {
        event.writeTo(channel)
    }

    journal.applyBy(handler) throws Exception {
        handler.handle(headPage.head().event())
        headPage = headPage.remove()
        tailPage = tailPage.saveCheckpoint(headPage.head())
    }

    journal.close()

    case Append_Event:
    case Delete_Event:


    */

}
