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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessors;
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.io.File;
import java.util.LinkedList;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class SequenceLoaderTest extends FileTestContext {
    @Test
    public void initializeLoad() throws Exception {
        dir = testDir("initializeLoad");
        SequenceLoader<String> loader = new SequenceLoader<String>(dir, Accessors.STRING, Cursor.NULL);

        LinkedList<LinkedPage<String>> linkedPages = loader.load();
        assertThat(linkedPages.size(), is(1));
    }

    @Test
    public void loadExistAndRemovePageContainsLastSequenceTail() throws Exception {
        dir = testDir("loadExistAndRemovePageContainsLastSequenceTail");

        new File(dir, "0").createNewFile();
        new File(dir, "16").createNewFile();

        SequenceLoader<String> loader = new SequenceLoader<String>(dir, Accessors.STRING, new Cursor(16L));
        LinkedList<LinkedPage<String>> linkedPages = loader.load();
        assertThat(linkedPages.size(), is(1));
        assertThat(new File(dir, "16").exists(), is(false));
    }

    @Test
    public void loadExistWithoutRemoving() throws Exception {
        dir = testDir("loadExistWithoutRemoving");

        new File(dir, "0").createNewFile();

        SequenceLoader<String> loader = new SequenceLoader<String>(dir, Accessors.STRING, new Cursor(16L));
        LinkedList<LinkedPage<String>> linkedPages = loader.load();
        assertThat(linkedPages.size(), is(1));
    }
}
