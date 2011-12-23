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

package com.github.zhongl.nio;

import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StoresTest extends FileBase {

    private FileChannelFactory factory;
    private FileChannel channel;
    private Stores stores;

    @Before
    public void setUp() throws Exception {
        file = testFile("store");
        stores = new Stores();
        factory = mock(FileChannelFactory.class);
        channel = spy(FileChannels.channel(file, 1024));
        when(factory.create()).thenReturn(channel);
    }

    @Test
    public void getOrMap() throws Exception {
        when(factory.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        Store buffer0 = stores.getOrCreateBy(factory);
        Store buffer1 = stores.getOrCreateBy(factory);

        verify(factory, times(1)).create();
        assertThat(buffer0, is(buffer1));
    }

    @Test
    public void writeAndRead() throws Exception {
        when(factory.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        Store buffer = stores.getOrCreateBy(factory);
        Accessor<byte[]> accessor = spy(CommonAccessors.BYTES);

        byte[] bytes = new byte[10];
        buffer.writeBy(accessor, 0, bytes);

        assertThat(buffer.readBy(accessor, 0), is(bytes));

    }

    @Test
    public void tryRelease() throws Exception {
        when(factory.maxIdleTimeMillis()).thenReturn(5L);
        stores.getOrCreateBy(factory); // first create
        stores.getOrCreateBy(factory); // no create

        Thread.sleep(5L);

        triggerTryRelease();

        stores.getOrCreateBy(factory); // second create

        verify(factory, times(2)).create();
    }

    @Test
    public void forceRelease() throws Exception {
        when(factory.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        Store buffer = stores.getOrCreateBy(factory);// first create
        stores.getOrCreateBy(factory); // no create

        Thread.sleep(5L);

        triggerTryRelease();

        buffer.writeBy(CommonAccessors.BYTES, 0, new byte[10]); // update last access time
        triggerForceRelease();

        stores.getOrCreateBy(factory); // second create

        verify(factory, times(2)).create();
    }

    @After
    public void tearDown() throws Exception {
        stores.clear();
    }

    private void triggerForceRelease() throws IOException {
        try {
            FileChannelFactory mapper0 = mock(FileChannelFactory.class);
            when(mapper0.create()).thenThrow(new OutOfMemoryError());
            stores.getOrCreateBy(mapper0); // force release
        } catch (OutOfMemoryError e) { }
    }

    private void triggerTryRelease() throws IOException {
        FileChannelFactory mapper0 = mock(FileChannelFactory.class);
        when(mapper0.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        FileChannel channel = spy(FileChannels.channel(file, 1024));
        when(mapper0.create()).thenReturn(channel);
        stores.getOrCreateBy(mapper0); // try release
    }
}
