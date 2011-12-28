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

package com.github.zhongl.page;

import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileBase {

    private Page<String> page;

    @Test
    public void appendAndGet() throws Exception {
        dir = testDir("appendAndGet");
        dir.mkdirs();
        int capacity = 4096;
        Recorder<String> recorder = Recorders.STRING;
        page = new Page<String>(new File(dir, "0"), capacity, recorder);

        String record = "record";
        Cursor cursor = page.append(record);
        assertThat(page.get(cursor), is(record));
        // TODO appendAndGet
    }

    @Override
    @After
    public void tearDown() throws Exception {
        page.close();
        super.tearDown();
    }
}
