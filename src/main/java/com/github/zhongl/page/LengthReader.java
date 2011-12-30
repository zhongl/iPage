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

package com.github.zhongl.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class LengthReader<T> implements Accessor.Reader<T> {
    @Override
    public final T readFrom(ReadableByteChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.rewind();
        channel.read(buffer);
        buffer.flip();
        int length = buffer.getInt();
        return readBodyFrom(channel, length);
    }

    protected abstract T readBodyFrom(ReadableByteChannel channel, int length) throws IOException;

}
