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
import com.google.common.io.Files;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileBase {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");
        File page0File = new File(dir, "0");
        int capacity = 22;

        // page0
        Page page0 = new Page(page0File, capacity);
        ByteBuffer event = ByteBuffer.wrap(new byte[] {1});

        assertThat(page0.append(event.duplicate()), is(page0));

        // page1
        Page page1 = page0.append(event.duplicate());
        assertThat(page1, is(not(page0)));

        page1.append(event.duplicate());
        File page1File = new File(dir, 28 + "");
        assertThat(page1File.exists(), is(true));

        Cursor cursor = page0.head();
        assertThat(cursor.get(), is(event));

        assertThat(page0.remove(), is(page0));
        // page2
        Page page2 = page1.saveCheckpoint(cursor.position());
        assertThat(page2, is(not(page1)));

        cursor = page0.head();
        assertThat(cursor.get(), is(event));

        assertThat(page0.remove(), is(page1));
        assertThat(page0File.exists(), is(false));

        page2.saveCheckpoint(cursor.position());
        File page2File = new File(dir, 63 + "");
        assertThat(page2File.exists(), is(true));

        cursor = page1.head();
        assertThat(cursor.get(), is(event));
        assertThat(page1.remove(), is(page1));

        cursor = page1.head();
        assertThat(cursor.get(), is(ByteBuffer.wrap(new byte[0]))); // assert cursor skip checkpoint

        assertThat(page1.remove(), is(page2));
        assertThat(page1File.exists(), is(false));

        cursor = page2.head();
        assertThat(cursor.get(), is(ByteBuffer.wrap(new byte[0]))); // assert cursor skip checkpoint
        assertThat(page2.remove(), is(page2));

        try {
            page2.head();
            fail("Should EOF.");
        } catch (EOFException e) { }

        page2.close();
    }

    @Test
    public void neitherRecoveryNorLastCheckpoint() throws Exception {
        dir = testDir("neitherRecoveryNorLastCheckpoint");
        file = new File(dir, "4096");
        Page page = new Page(file, 4096);
        page.append(ByteBuffer.wrap("something".getBytes()));
        page.close();
        page = new Page(file, 4096);
        assertThat(page.recoverAndGetLastCheckpoint(), is(-1L));
        page.close();
    }

    @Test
    public void recoveryButNoLastCheckpoint() throws Exception {
        dir = testDir("recoveryButNoLastCheckpoint");
        file = new File(dir, "4096");
        Files.write(new byte[1], file); // write broken data
        Page page = new Page(file, 7);
        assertThat(page.recoverAndGetLastCheckpoint(), is(-1L));
        page.close();
        assertThat(file.length(), is(0L));
    }

    @Test
    public void getLastCheckpointButNoRecovery() throws Exception {
        dir = testDir("getLastCheckpointButNoRecovery");
        file = new File(dir, "4096");
        Page page = new Page(file, 4096);
        page.saveCheckpoint(16L);
        page.saveCheckpoint(32L);
        page.close();

        page = new Page(file, 4096);
        assertThat(page.recoverAndGetLastCheckpoint(), is(32L));
        page.close();
    }
}
