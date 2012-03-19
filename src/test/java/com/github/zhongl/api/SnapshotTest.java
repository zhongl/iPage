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

package com.github.zhongl.api;

import com.github.zhongl.codec.Codec;
import com.github.zhongl.index.IndexCodec;
import com.github.zhongl.index.Key;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class SnapshotTest extends FileTestContext {
    @Test
    public void usage() throws Exception {
        dir = testDir("usage");
        File headFile = new File(dir, "HEAD");
        File pagesDir = new File(dir, "pages");

        IndexCodec indexCodec = mock(IndexCodec.class);
        Codec<Entry<Key, Integer>> entryCodec = mock(Codec.class);

        Snapshot<Integer> snapshot = new Snapshot<Integer>(dir, indexCodec, entryCodec);

        snapshot.updateAndCleanUp();

        String snapshotFileName = Files.readFirstLine(headFile, Charset.defaultCharset());
        assertThat(snapshotFileName, is(not("null.s")));
        assertThat(new File(pagesDir, snapshotFileName).exists(), is(true));

        assertThat(new File(pagesDir, "null.s").exists(), is(false));
    }
}

