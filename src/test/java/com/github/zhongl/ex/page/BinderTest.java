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

package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.codec.ComposedCodecBuilder;
import com.github.zhongl.ex.codec.LengthCodec;
import com.github.zhongl.ex.codec.StringCodec;
import com.github.zhongl.util.FileTestContext;
import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;

import static com.github.zhongl.util.FileAsserter.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BinderTest extends FileTestContext {

    private Binder binder;

    @Test
    public void append() throws Exception {
        dir = testDir("append");
        final Codec codec = ComposedCodecBuilder.compose(new StringCodec())
                                                .with(LengthCodec.class)
                                                .build();
        binder = new Binder(dir) {
            @Override
            protected Page newPage(File file, long number) {
                return new Page(file, number, 4096, codec) {
                    @Override
                    protected Batch newBatch(File file, int position, Codec codec, int estimateBufferSize) {
                        return new DefaultBatch(file, position, codec, estimateBufferSize);
                    }
                };
            }

            @Override
            protected long newPageNumber(@Nullable Page last) {
                return last == null ? 0L : last.number() + last.file().length();
            }
        };

        String value = "value";

        Cursor<String> cursor = binder.append(value, true);
        assertThat(cursor.get(), is(value));
        assertExist(new File(dir, "0")).contentIs(length(5), string(value));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (binder != null) binder.close();
        super.tearDown();
    }
}
