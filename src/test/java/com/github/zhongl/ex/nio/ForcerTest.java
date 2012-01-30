/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ex.nio;

import com.github.zhongl.util.FileAsserter;
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class ForcerTest extends FileTestContext {
    @Test
    public void write() throws Exception {
        file = testFile("write");

        FileChannel channel = new RAFileChannel(file);
        byte[] bytes = {1, 2};

        try {
            forcer().force(channel, ByteBuffer.wrap(bytes));
        } finally {
            channel.close();
        }

        FileAsserter.assertExist(file).contentIs(bytes);
    }

    protected abstract Forcer forcer();
}
