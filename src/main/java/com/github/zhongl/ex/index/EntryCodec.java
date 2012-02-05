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

package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class EntryCodec implements Codec {

    static final int LENGTH = 16 + 8;

    @Override
    public ByteBuffer encode(Object instance) {
        Entry<Md5Key, Offset> entry = (Entry<Md5Key, Offset>) instance;
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH);
        buffer.put(entry.key().bytes());
        buffer.putLong(entry.value().value());
        return (ByteBuffer) buffer.flip();
    }

    @Override
    public Entry<Md5Key, Offset> decode(ByteBuffer buffer) {
        byte[] bytes = new byte[Md5Key.BYTE_LENGTH];
        buffer.get(bytes);
        return new Entry<Md5Key, Offset>(new Md5Key(bytes), new Offset(buffer.getLong()));
    }

    @Override
    public boolean supports(Class<?> type) {
        return Entry.class.equals(type);
    }
}
