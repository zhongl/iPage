package com.github.zhongl.journal1;

import com.github.zhongl.util.Checksums;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Codecs {

    static final int CODEC_CODE_CRC32_LENGTH = 1 + 8 + 4;

    private final Codec[] codeTable;

    private final boolean directBuffer;

    public Codecs(Codec... codecs) {
        this(false, codecs);
    }

    public Codecs(boolean directBuffer, Codec... codes) {
        Preconditions.checkArgument(codes.length <= Byte.MAX_VALUE, "Less than %s codec can be register.", Byte.MAX_VALUE);
        this.directBuffer = directBuffer;
        this.codeTable = codes;
    }

    ByteBuffer toBuffer(Object instance) {
        ByteBuffer buffer = null;
        for (int i = 0; i < codeTable.length; i++) {
            Codec codec = codeTable[i];
            if (codec.supports(instance)) {
                buffer = codec.toBuffer(instance);
                ByteBuffer packed = ByteBuffer.allocate(CODEC_CODE_CRC32_LENGTH + buffer.limit());
                packed.put((byte) i) // codec code
                        .putLong(Checksums.checksum(buffer.duplicate())) // TODO use unsign int
                        .putInt(buffer.limit()) // length
                        .put(buffer) // body
                        .flip();
                return packed;
            }
        }

        throw new IllegalStateException("Can't find the codec of " + instance);
    }

    <T> T toInstance(ByteBuffer buffer) {
        Codec codec = codeTable[buffer.get()];
        buffer.getLong();// skip checksum
        int length = buffer.getInt();
        buffer.limit(buffer.position() + length);
        ByteBuffer body = buffer.slice();
        return (T) codec.toInstance(buffer);
    }

    <T> T validateAndToInstance(ByteBuffer buffer) {
        Codec codec = codeTable[buffer.get()];
        long checksum = buffer.getLong();// TODO use unsign int
        int length = buffer.getInt();
        buffer.limit(buffer.position() + length);
        ByteBuffer body = buffer.slice();
        Checksums.validate(body.duplicate(), checksum);
        return (T) codec.toInstance(body);
    }

}
