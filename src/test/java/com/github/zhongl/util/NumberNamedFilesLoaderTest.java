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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static org.hamcrest.Matchers.hasItems;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class NumberNamedFilesLoaderTest extends FileBase {
    @Test
    public void load() throws Exception {
        dir = testDir("load");
        dir.mkdirs();
        new File(dir, "0").createNewFile();
        new File(dir, "1").createNewFile();
        FileHandler<String> handler = new FileHandler<String>() {

            @Override
            public String handle(File file) {
                return file.getName();
            }
        };
        ArrayList<String> collection = new NumberNamedFilesLoader<String>(dir, handler).loadTo(new ArrayList<String>());
        Assert.assertThat(collection, hasItems("0", "1"));
    }
}
