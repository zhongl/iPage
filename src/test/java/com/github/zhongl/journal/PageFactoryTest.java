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

import java.io.File;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageFactoryTest extends FileBase {

    @Test
    public void create() throws Exception {
        dir = testDir("create");
        PageFactory factory = new PageFactory(dir, new EventAccessor());
        factory.create();
        assertThat(new File(dir, "0").exists(), is(true));
    }

    @Test
    public void load() throws Exception {
        dir = testDir("load");
        EventAccessor accessor = new EventAccessor();
        new Page(new File(dir, "0"), accessor).fix();
        new File(dir, "1").createNewFile();

        PageFactory factory = new PageFactory(dir, accessor);
        List<Page> pages = factory.unappliedPages();
        assertThat(new File(dir, "1").exists(), is(false));
        assertThat(pages.size(), is(1));
        factory.create();
        assertThat(new File(dir, "1").exists(), is(true));
    }
}
