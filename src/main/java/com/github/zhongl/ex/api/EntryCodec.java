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

package com.github.zhongl.ex.api;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.util.Entry;

import java.nio.ByteBuffer;

import static com.github.zhongl.ex.nio.ByteBuffers.lengthOf;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class EntryCodec implements Codec {
    @Override
    public ByteBuffer encode(Object instance) {
        Entry<Md5Key, byte[]> entry = (Entry<Md5Key, byte[]>) instance;
        return (ByteBuffer) ByteBuffer.allocate(Md5Key.BYTE_LENGTH + entry.value().length)
                                      .put(entry.key().bytes())
                                      .put(entry.value())
                                      .flip();
    }

    @Override
    public Entry<Md5Key, byte[]> decode(ByteBuffer buffer) {
        byte[] md5 = new byte[Md5Key.BYTE_LENGTH];
        byte[] bytes = new byte[lengthOf(buffer) - Md5Key.BYTE_LENGTH];
        buffer.get(md5);
        buffer.get(bytes);
        return new Entry<Md5Key, byte[]>(new Md5Key(md5), bytes);
    }

    @Override
    public boolean supports(Class<?> type) {
        return Entry.class.equals(type);
    }
}
