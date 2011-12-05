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

package com.github.zhongl.util;

import com.google.common.base.Joiner;
import org.junit.After;
import org.junit.Before;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class FileBase {
    private static final String BASE_ROOT = "target/tmpTestFiles/";
    protected File file;

    @Before
    public void setUp() throws Exception {
        new File(BASE_ROOT).mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        if (file != null && file.exists()) file.delete();
    }

    protected File testFile(String name) {
        return new File(BASE_ROOT, Joiner.on('.').join(getClass().getSimpleName(), name));
    }
}
