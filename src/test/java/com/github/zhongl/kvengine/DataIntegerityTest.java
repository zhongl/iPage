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

package com.github.zhongl.kvengine;

import com.github.zhongl.util.FileBase;
import org.junit.Test;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DataIntegerityTest extends FileBase {

    private DataIntegerity dataIntegerity;


    @Test(expected = UnsafeDataStateException.class)
    public void unsafeDataState() throws Exception {
        dir = testDir("unsafeDataState");
        dir.mkdirs();
        dataIntegerity = new DataIntegerity(dir);
        dataIntegerity.validate();
    }

    @Test
    public void safeDataState() throws Exception {
        dir = testDir("safeDataState");
        dir.mkdirs();
        dataIntegerity = new DataIntegerity(dir);
        dataIntegerity.safeClose();
        dataIntegerity.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void nonExistDir() throws Exception {
        dir = testDir("nonExistDir");
        new DataIntegerity(dir);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidDir() throws Exception {
        dir = testDir("invalidDir");
        dir.createNewFile();
        new DataIntegerity(dir);
    }
}
