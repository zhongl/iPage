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
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileBase {

    private Page page;

    @Test
    public void main() throws Exception {
        dir = testDir("main");
        file = new File(dir, "0");
        int capacity = Page.FLAG_CRC32_LENGTH + 2;

        page = new Page(file, capacity);
        ByteBuffer event = ByteBuffer.wrap(new byte[] {1});

        assertThat(page.append(event.duplicate()), is(page));

        Page newPage = page.append(event.duplicate());
        assertThat(newPage, is(not(page)));

        newPage.append(event.duplicate());
        assertThat(new File(dir, 2 * (Page.FLAG_CRC32_LENGTH + 1) + "").exists(), is(true));

        Cursor cursor = page.head();
        assertThat(cursor.get(), is(event));

        assertThat(page.remove(), is(page));
        assertThat(page.remove(), is(newPage));
        assertThat(file.exists(), is(false));

        newPage.close();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (page != null) page.close();
        super.tearDown();
    }
}
