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

import com.github.zhongl.nio.CommonAccessors;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.util.FileBase;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class GarbageCollectTest extends FileBase {

    @Test
    public void garbageCollect() throws Exception {
        dir = testDir("garbageCollect");
        BlockingKVEngine<String> engine = new BlockingKVEngine<String>(
                KVEngine.<String>baseOn(dir)
                        .valueAccessor(CommonAccessors.STRING)
                        .flushElapseMilliseconds(10L)
                        .minimzieCollectLength(4096)
                        .maximizeChunkCapacity(4096)
                        .startAutoGarbageCollectOnStartup(true)
                        .build()
        );
        engine.startup();

        String value = "0123456789ab";
        for (int i = 0; i < 258; i++) {
            engine.put(Md5Key.generate(Ints.toByteArray(i)), value);
        }

        File ipage = new File(dir, KVEngine.IPAGE_DIR);

        assertThat(new File(ipage, "0").length(), is(4096L));
        assertThat(new File(ipage, "4096").length(), is(4096L));
        assertThat(new File(ipage, "8192").length(), is(4096L));

        for (int i = 5; i < 257; i++) {
//            System.out.println(i);
            engine.remove(Md5Key.generate(Ints.toByteArray(i))); // remove 0 - 8192
        }

        Thread.sleep(500L);

        engine.shutdown();
        assertThat(new File(ipage, "0").length(), is(160L));
        assertThat(new File(ipage, "8192").length(), is(64L));
    }

}
