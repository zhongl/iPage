package com.github.zhongl.codec;

import java.nio.ByteBuffer;

import static com.github.zhongl.codec.ByteBuffers.lengthOf;
import static com.github.zhongl.util.Checksums.checksum;
import static com.github.zhongl.util.Checksums.validate;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChecksumCodec extends DecoratedCodec {

    // TODO use unsign int
    public static final int CHECKSUM_LENGTH = 8;

    public ChecksumCodec(Codec codec) {
        super(codec);
    }

    public ByteBuffer encode(Object instance) {
        ByteBuffer body = delegate.encode(instance);
        ByteBuffer encoded = ByteBuffer.allocate(CHECKSUM_LENGTH + lengthOf(body));
        encoded.putLong(checksum(body)).put(body).flip();
        return encoded;
    }

    @Override
    public <T> T decode(ByteBuffer buffer) {
        long checksum = buffer.getLong();
        T instance = delegate.decode(buffer);
        validate(delegate.encode(instance), checksum);
        return instance;
    }

}
