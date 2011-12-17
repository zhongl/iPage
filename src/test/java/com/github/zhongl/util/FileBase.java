/*
 * Copyright 2011 zhongl
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

package com.github.zhongl.util;

import com.google.common.base.Joiner;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class FileBase {
    private static final String BASE_ROOT = "target/tmpTestFiles/";
    protected File file;
    protected File dir;

    @Before
    public void setUp() throws Exception {
        new File(BASE_ROOT).mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        if (file != null && file.exists()) file.delete();
    }

    private void delete(File file) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) delete(f);
            if (!file.delete()) throw new IOException("Can't delete dir " + file);
        }
        if (file.isFile() && !file.delete()) throw new IOException("Can't delete file " + file);
    }

    protected File testDir(String name) throws IOException {
        File file = new File(BASE_ROOT, Joiner.on('.').join(getClass().getSimpleName(), name));
        if (file.exists()) delete(file);
        return file;
    }

    protected void assertNotExistFile(String name) {assertThat(new File(dir, name).exists(), is(false));}

    protected void assertExistFile(String name) {assertThat(new File(dir, name).exists(), is(true));}

    protected File testFile(String name) {
        return new File(BASE_ROOT, Joiner.on('.').join(getClass().getSimpleName(), name));
    }
}
