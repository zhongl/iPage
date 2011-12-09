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

package com.github.zhongl.ipage;

import com.github.zhongl.integerity.Validator;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkTest extends ChunkBase {

    @Test
    public void appendAndGet() throws Exception {
        file = testFile("appendAndGet");
        newChunk();

        String record = "record";
        long offset = chunk.append(record);
        assertThat(offset, is(0L));
        assertThat(chunk.get(offset), is(record));
    }

    @Test
    public void nextCursor() throws Exception {
        file = testFile("nextCursor");
        newChunk();

        for (int i = 0; i < 10; i++) {
            chunk.append("" + i);
        }

        Cursor<String> cursor = Cursor.begin(chunk.beginPositionInIPage());
        for (int i = 0; i < 10; i++) {
            cursor = chunk.next(cursor);
            assertThat(cursor.lastValue(), is(i + ""));
        }
    }

    @Test
    public void validate() throws Exception {
        file = testFile("checkCRC");
        newChunk();

        for (int i = 0; i < 10; i++) {
            chunk.append("" + i);
        }

        Validator<String, IOException> validator = new Validator<String, IOException>() {
            @Override
            public boolean validate(String value) throws IOException {
                return !value.equals(7 + "");
            }
        };

        assertThat(chunk.validateOrRecoverBy(validator), is(false));
    }


    @Test(expected = IllegalStateException.class)
    public void appendAfterErase() throws Exception {
        file = testFile("appendAfterErase");
        newChunk();
        chunk.erase();
        chunk.append("Oops");
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterErase() throws Exception {
        file = testFile("getAfterErase");
        newChunk();
        chunk.erase();
        chunk.get(0L);
    }

    @Test(expected = IllegalStateException.class)
    public void nextAfterErase() throws Exception {
        file = testFile("nextAfterErase");
        newChunk();
        chunk.erase();
        chunk.next(null);
    }

    @Test(expected = IllegalStateException.class)
    public void getBeginPositionAfterErase() throws Exception {
        file = testFile("getBeginPositionAfterErase");
        newChunk();
        chunk.erase();
        chunk.beginPositionInIPage();
    }

    @Test(expected = IllegalStateException.class)
    public void getEndPositionAfterErase() throws Exception {
        file = testFile("getEndPositionAfterErase");
        newChunk();
        chunk.erase();
        chunk.endPositionInIPage();
    }

    @Test
    public void getByInvalidOffset() throws Exception {
        file = testFile("getByInvalidOffset");
        newChunk();
        chunk.append("record");
        assertThat(chunk.get(1L), is(nullValue()));
    }

    @Test
    public void closeAfterErase() throws Exception {
        file = testFile("closeAfterErase");
        newChunk();
        chunk.erase();
        chunk.close();
    }

    @Test
    public void eraseAfterErase() throws Exception {
        file = testFile("eraseAfterErase");
        newChunk();
        chunk.erase();
        chunk.erase();
    }

    @Test
    public void flushAfterErase() throws Exception {
        file = testFile("flushAfterErase");
        newChunk();
        chunk.erase();
        chunk.flush();
    }

    @Test
    public void flushAfterClose() throws Exception {
        file = testFile("cleanAfterClose");
        newChunk();
        chunk.close();
        chunk.flush();
    }

    @Test
    public void closeAfterClose() throws Exception {
        file = testFile("cleanAfterClose");
        newChunk();
        chunk.close();
        chunk.close();
    }

}
