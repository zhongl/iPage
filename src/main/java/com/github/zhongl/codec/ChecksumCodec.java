package com.github.zhongl.codec;

import com.github.zhongl.util.Checksums;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChecksumCodec extends DecoratedCodec {

    // TODO use unsign int
    public static final int CHECKSUM_LENGTH = 8;

    public ChecksumCodec(Codec codec) {
        super(codec);
    }

    public ByteBuffer encode(Object instance) {
        ByteBuffer body = delegate.encode(instance);
        int length = body.limit() - body.position();
        ByteBuffer encoded = ByteBuffer.allocate(CHECKSUM_LENGTH + length);
        encoded.putLong(Checksums.checksum(body.duplicate())).put(body).flip();
        return encoded;
    }

    @Override
    public <T> T decode(ByteBuffer buffer) {
        long checksum = buffer.getLong();
        T instance = delegate.decode(buffer);
        Checksums.validate(delegate.encode(instance), checksum);
        return instance;
    }

}
