/*
 * Copyright 2012 zhongl
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
    public void append() throws Exception {
        dir = testDir("append");
        file = new File(dir, "0");
        int capacity = Page.FLAG_CRC32_LENGTH + 2;

        page = new Page(file, capacity);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[1]);

        assertThat(page.append(buffer), is(page));

        Page newPage = page.append(buffer);
        assertThat(newPage, is(not(page)));

        page = newPage;
        newPage.append(buffer);
        assertThat(new File(dir, 2 * (Page.FLAG_CRC32_LENGTH + 1) + "").exists(), is(true));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (page != null) page.close();
        super.tearDown();
    }
}
