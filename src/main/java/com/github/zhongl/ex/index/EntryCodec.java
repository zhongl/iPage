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
        return type.equals(Entry.class);
    }
}
