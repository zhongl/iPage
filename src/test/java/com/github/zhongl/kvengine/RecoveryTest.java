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

import com.github.zhongl.buffer.CommonAccessors;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RecoveryTest extends FileBase {

    private KVEngine<String> engine;

    @After
    public void tearDown() throws Exception {
        if (engine != null) engine.shutdown();
    }

    @Test
    public void dataIsOkButDotSafeNotExist() throws Exception {
        dir = testDir("dataIsOkButDotSafeNotExist");
        engine = KVEngine.<String>baseOn(dir).valueAccessor(CommonAccessors.STRING).build();
        engine.startup();

        String value = "value";
        Md5Key key = Md5Key.generate(value.getBytes());
        Sync<String> callback = new Sync<String>();
        engine.put(key, value, callback);
        callback.get();
        engine.shutdown();

        boolean delete = new File(dir, ".safe").delete(); // delete .safe file
        assertThat(delete, is(true));

        engine = KVEngine.<String>baseOn(dir).valueAccessor(CommonAccessors.STRING).build();

    }
}
