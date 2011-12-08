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

import com.github.zhongl.accessor.CommonAccessors;
import com.github.zhongl.integerity.Validator;
import com.github.zhongl.util.FileBase;
import com.github.zhongl.util.FileContentAsserter;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageTest extends FileBase {
    public static final boolean CLOSE = true;
    public static final boolean FLUSH = false;

    private IPage<String> iPage;

    @After
    public void tearDown() throws Exception {
        if (iPage != null) iPage.close();
    }

    @Test
    public void createAndAppendAndClose() throws Exception {
        dir = testDir("createAndAppendAndClose");
        assertThat(dir.exists(), is(false));
        newIPage();
        assertAppendAndDurableBy(CLOSE);
    }

    @Test
    public void createAndAppendAndFlush() throws Exception {
        dir = testDir("createAndAppendAndFlush");
        assertThat(dir.exists(), is(false));
        newIPage();
        assertAppendAndDurableBy(FLUSH);
    }

    @Test
    public void getAfterAppended() throws Exception {
        dir = testDir("getAfterAppended");

        newIPage();
        assertThat(iPage.get(0L), is(nullValue()));

        String record = "lastValue";
        long offset = iPage.append(record);

        assertThat(iPage.get(offset), is(record));
    }

    @Test
    public void getFromNonAppendingChunk() throws Exception {
        dir = testDir("getFromNonAppendingChunk");
        newIPage();
        String record = "0123456789ab";
        for (int i = 0; i < 257; i++) {
            iPage.append(record);
        }
        assertExistFile("0");
        assertExistFile("4096");

        assertThat(iPage.get(0L), is(record));
        assertThat(iPage.get(4080L), is(record));
        assertThat(iPage.get(4096L), is(record));
    }

    @Test
    public void loadExist() throws Exception {
        dir = testDir("loadExist");

        // create a iPage with two chunk
        newIPage();
        String record = "0123456789ab";
        for (int i = 0; i < 257; i++) {
            iPage.append(record);
        }
        iPage.close();

        assertExistFile("0");
        assertExistFile("4096");

        // load and verify
        newIPage();
        String newRecord = "newRecord";
        long offset = iPage.append(newRecord);

        assertThat(iPage.get(0L), is(record));
        assertThat(iPage.get(offset), is(newRecord));
    }

    @Test
    public void recovery() throws Exception {
        dir = testDir("recovery");
        newIPage();

        for (int i = 0; i < 10; i++) {
            iPage.append("" + i);
        }
        assertThat(iPage.get(35L), is(7 + ""));

        Validator<String, IOException> stringValidator = new Validator<String, IOException>() {
            @Override
            public boolean validate(String value) throws IOException {
                return !value.equals(7 + "");
            }
        };
        iPage.validateOrRecoverBy(stringValidator);

        assertThat(iPage.get(30L), is(6 + ""));
        assertThat(iPage.get(35L), is(nullValue()));
    }

    @Test
    public void nextCursor() throws Exception {
        dir = testDir("nextCursor");
        newIPage();

        String record = "0123456789ab";
        for (int i = 0; i < 257; i++) {
            iPage.append(record);
        }

        Cursor<String> cursor = new Cursor<String>(-1L, null);
        for (int i = 0; i < 257; i++) {
            cursor = iPage.next(cursor);
            assertThat(cursor.lastValue, is(record));
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidChunkCapacity() throws Exception {
        dir = testDir("invalidChunkCapacity");
        IPage.baseOn(dir).chunkCapacity(4095);
    }

    @Test(expected = IllegalStateException.class)
    public void repeatSetupChunkCapcity() throws Exception {
        dir = testDir("repeatSetupChunkCapcity");
        IPage.baseOn(dir).chunkCapacity(4096).chunkCapacity(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidDir() throws Exception {
        dir = testDir("invalidDir");
        dir.createNewFile();
        IPage.baseOn(dir);
    }

    private void newIPage() throws IOException {
        iPage = IPage.<String>baseOn(dir).accessor(CommonAccessors.STRING).chunkCapacity(4096).build();
    }

    private void assertAppendAndDurableBy(boolean close) throws IOException {
        assertThat(iPage.append("item1"), is(0L));
        assertThat(iPage.append("item2"), is(9L));
        if (close) {
            iPage.close();
        } else {
            iPage.flush();
        }
        byte[] expect = ChunkContentUtils.concatToChunkContentWith("item1".getBytes(), "item2".getBytes());
        FileContentAsserter.of(new File(dir, "0")).assertIs(expect);
    }

}
