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
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.page.DefaultCursor;
import com.github.zhongl.ex.util.Entry;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class EntryCodec implements Codec {

    static final int LENGTH = 16 + 8 + 4;

    @Override
    public ByteBuffer encode(Object instance) {
        Entry<Md5Key, Cursor> entry = (Entry<Md5Key, Cursor>) instance;
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH);
        buffer.put(entry.key().bytes());
        DefaultCursor cursor = (DefaultCursor) entry.value();
        buffer.putLong(cursor.offset());
        buffer.putInt(cursor.length());
        return (ByteBuffer) buffer.flip();
    }

    @Override
    public Entry<Md5Key, Cursor> decode(ByteBuffer buffer) {
        byte[] bytes = new byte[Md5Key.BYTE_LENGTH];
        buffer.get(bytes);
        return new Entry<Md5Key, Cursor>(new Md5Key(bytes), new DefaultCursor(buffer.getLong(), buffer.getInt()));
    }

    @Override
    public boolean supports(Class<?> type) {
        return Entry.class.equals(type);
    }
}
