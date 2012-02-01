package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Md5KeyCodec implements Codec {

    @Override
    public ByteBuffer encode(Object instance) {

        return ByteBuffer.wrap(((Md5Key) instance).bytes());
    }

    @Override
    public Md5Key decode(ByteBuffer buffer) {
        byte[] bytes = new byte[Md5Key.BYTE_LENGTH];
        buffer.get(bytes);
        return new Md5Key(bytes);
    }

    @Override
    public boolean supports(Class<?> type) {
        return Md5Key.class.equals(type);
    }
}
