package com.github.zhongl.ex.codec;

import com.github.zhongl.ex.nio.ByteBuffers;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BytesCodec implements Codec {
    @Override
    public ByteBuffer encode(Object instance) {
        return ByteBuffer.wrap((byte[]) instance);
    }

    @Override
    public byte[] decode(ByteBuffer buffer) {
        int length = ByteBuffers.lengthOf(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    @Override
    public boolean supports(Class<?> type) {
        return byte[].class.isAssignableFrom(type);
    }

}
