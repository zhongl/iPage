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

package com.github.zhongl.buffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappedDirectBuffersTest {

    private DirectBufferMapper mapper;
    private MappedByteBuffer mappedByteBuffer;
    private MappedDirectBuffers mappedDirectBuffers;

    @Before
    public void setUp() throws Exception {
        mappedDirectBuffers = new MappedDirectBuffers();
        mapper = mock(DirectBufferMapper.class);
        mappedByteBuffer = mock(MappedByteBuffer.class);
        when(mapper.map()).thenReturn(mappedByteBuffer);
        when(mappedByteBuffer.duplicate()).thenReturn(mappedByteBuffer);
    }

    @Test
    public void getOrMap() throws Exception {
        when(mapper.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        MappedDirectBuffer buffer0 = mappedDirectBuffers.getOrMapBy(mapper);
        MappedDirectBuffer buffer1 = mappedDirectBuffers.getOrMapBy(mapper);

        verify(mapper, times(1)).map();
        assertThat(buffer0, is(buffer1));
    }

    @Test
    public void writeAndRead() throws Exception {
        when(mapper.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        MappedDirectBuffer buffer = mappedDirectBuffers.getOrMapBy(mapper);
        Accessor<byte[]> accessor = spy(CommonAccessors.BYTES);

        byte[] bytes = new byte[10];
        buffer.writeBy(accessor, 0, bytes);

        verify(accessor).write(bytes, mappedByteBuffer);

        when(mappedByteBuffer.getInt()).thenReturn(10);
        buffer.readBy(accessor, 0);

        verify(accessor).read(mappedByteBuffer);
    }


    @Test(expected = UnsupportedOperationException.class)
    public void callMappedByteBufferFlush() throws Exception {
        when(mapper.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        mappedDirectBuffers.getOrMapBy(mapper).flush();
    }

    @Test
    public void tryRelease() throws Exception {
        when(mapper.maxIdleTimeMillis()).thenReturn(5L);
        mappedDirectBuffers.getOrMapBy(mapper); // first map
        mappedDirectBuffers.getOrMapBy(mapper); // no map

        Thread.sleep(5L);

        triggerTryRelease();

        mappedDirectBuffers.getOrMapBy(mapper); // second map

        verify(mapper, times(2)).map();
    }

    @Test
    public void forceRelease() throws Exception {
        when(mapper.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        MappedDirectBuffer buffer = mappedDirectBuffers.getOrMapBy(mapper);// first map
        mappedDirectBuffers.getOrMapBy(mapper); // no map

        Thread.sleep(5L);

        triggerTryRelease();

        buffer.writeBy(CommonAccessors.BYTES, 0, new byte[10]); // update last access time
        triggerForceRelease();

        mappedDirectBuffers.getOrMapBy(mapper); // second map

        verify(mapper, times(2)).map();
    }

    @After
    public void tearDown() throws Exception {
        mappedDirectBuffers.clear();
    }

    private void triggerForceRelease() throws IOException {
        try {
            DirectBufferMapper mapper0 = mock(DirectBufferMapper.class);
            when(mapper0.map()).thenThrow(new OutOfMemoryError());
            mappedDirectBuffers.getOrMapBy(mapper0); // force release
        } catch (OutOfMemoryError e) { }
    }

    private void triggerTryRelease() throws IOException {
        DirectBufferMapper mapper0 = mock(DirectBufferMapper.class);
        when(mapper0.maxIdleTimeMillis()).thenReturn(Long.MAX_VALUE);
        MappedByteBuffer buffer = mock(MappedByteBuffer.class);
        when(mapper0.map()).thenReturn(buffer);
        mappedDirectBuffers.getOrMapBy(mapper0); // try release
    }
}
