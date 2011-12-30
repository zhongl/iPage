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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessor;
import com.github.zhongl.page.Accessors;
import com.github.zhongl.page.ReadOnlyChannels;
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class LinkedPageTest extends FileBase {

    private LinkedPage<String> linkedPage;

    @Test
    public void main() throws Exception {
        dir = testDir("main");
        dir.mkdirs();
        int capacity = 4096;
        Accessor<String> accessor = Accessors.STRING;
        linkedPage = new LinkedPage<String>(new File(dir, "0"), accessor, capacity, new ReadOnlyChannels());

        String record = "record";
        Cursor cursor = linkedPage.append(record);
        assertThat(linkedPage.get(cursor), is(record));
        assertThat(linkedPage.next(cursor), is(new Cursor(10L)));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        linkedPage.close();
        super.tearDown();
    }
}
