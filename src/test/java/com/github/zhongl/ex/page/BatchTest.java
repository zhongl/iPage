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
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.github.zhongl.util.FileTestContext;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;

import static com.github.zhongl.util.FileAsserter.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class BatchTest extends FileTestContext {
    private Page page = mock(Page.class);

    private Codec codec = ComposedCodecBuilder.compose(new StringCodec())
                                              .with(LengthCodec.class)
                                              .build();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        file = testFile("main");
        doReturn(file).when(page).file();
        doReturn(codec).when(page).codec();
    }

    @Test
    public void main() throws Exception {

        Batch batch = newBatch(codec, 0, 4096);

        FutureCallback<Cursor> ignore = FutureCallbacks.<Cursor>ignore();
        batch.append("1", ignore);
        batch.append("2", ignore);
        batch.writeAndForceTo(FileChannels.getOrOpen(file));
        FileChannels.closeChannelOf(file);

        assertExist(file).contentIs(length(1), string("1"), length(1), string("2"));
    }

    protected abstract Batch newBatch(Codec codec, int position, int estimateBufferSize);

}
