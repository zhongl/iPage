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
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineTest extends FileBase {

    protected BlockingKVEngine engine;

    @Test
    public void putAndGetAndRemove() throws Exception {
        dir = testDir("putAndGetAndRemove");
        newEngineAndStartup();

        byte[] value = "value".getBytes();
        Md5Key key = Md5Key.generate(value);

        assertThat(engine.put(key, value), is(nullValue()));
        assertThat(engine.get(key), is(value));
        assertThat(engine.remove(key), is(value));
    }

    @Test
    public void getAndRemoveNonExistKey() throws Exception {
        dir = testDir("getAndRemoveNonExistKey");
        newEngineAndStartup();
        Md5Key key = Md5Key.generate("non-exist".getBytes());
        assertThat(engine.get(key), is(nullValue()));
        assertThat(engine.remove(key), is(nullValue()));
    }

    @Test
    public void iterator() throws Exception {
        dir = testDir("valueIterator");

        engine = new BlockingKVEngine(
                KVEngine.baseOn(dir)
                        .groupCommit(true)
                        .flushElapseMilliseconds(10L)
                        .valueAccessor(CommonAccessors.BYTES)
                        .build());
        engine.startup();

        Iterator<byte[]> iterator = engine.valueIterator();
        assertThat(iterator.hasNext(), is(false));

        byte[] value0 = "value0".getBytes();
        engine.put(Md5Key.generate(value0), value0);
        byte[] value1 = "value1".getBytes();
        engine.put(Md5Key.generate(value1), value1);
        byte[] value2 = "value2".getBytes();
        engine.put(Md5Key.generate(value2), value2);

        iterator = engine.valueIterator();

        assertThat(iterator.next(), is(value0));
        assertThat(iterator.next(), is(value1));
        assertThat(iterator.next(), is(value2));

        byte[] value3 = "value3".getBytes();
        engine.put(Md5Key.generate(value0), value3);
        engine.remove(Md5Key.generate(value1));

        iterator = engine.valueIterator();

        assertThat(iterator.next(), is(value3));
        assertThat(iterator.next(), is(value2));
        assertThat(iterator.hasNext(), is(false));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (engine != null) engine.shutdown();
    }

    private void newEngineAndStartup() throws IOException {
        engine = new BlockingKVEngine(
                KVEngine.baseOn(dir)
                        .groupCommit(false)
                        .flushElapseMilliseconds(10L)
                        .valueAccessor(CommonAccessors.BYTES)
                        .build());
        engine.startup();
    }
}
