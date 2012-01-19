package com.github.zhongl.journal1;

import com.github.zhongl.util.Checksums;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Codecs {

    static final int CODEC_CODE_CRC32_LENGTH = 1 + 8 + 4;

    private final Map<Class<?>, CodecWithCode> codecMap;
    private final Codec[] codecTable;

    public Codecs(Codec... codecs) {
        checkArgument(codecs.length < Byte.MAX_VALUE, "Less than %s codec can be register.", Byte.MAX_VALUE - 1);
        this.codecTable = codecs;

        Codec[] newCodecs = Arrays.copyOf(codecs, codecs.length + 1);
        newCodecs[codecs.length] = Checkpoint.CODEC;
        Map<Class<?>, CodecWithCode> map = new HashMap<Class<?>, CodecWithCode>(codecs.length);
        for (int i = 0; i < codecs.length; i++) {
            map.put(codecs[i].supported(), new CodecWithCode((byte) (i), codecs[i]));
        }
        codecMap = Collections.unmodifiableMap(map);
    }

    ByteBuffer toBuffer(Object instance) {
        CodecWithCode codecWithCode = codecMap.get(instance.getClass());
        if (codecWithCode == null)
            throw new IllegalStateException("Can't find the codec of " + instance);

        ByteBuffer buffer = codecWithCode.codec.toBuffer(instance);
        ByteBuffer packed = ByteBuffer.allocate(CODEC_CODE_CRC32_LENGTH + buffer.limit());
        packed.put(codecWithCode.code)
                .putLong(Checksums.checksum(buffer.duplicate())) // TODO use unsign int
                .putInt(buffer.limit()) // length
                .put(buffer) // body
                .flip();
        return packed;
    }

    <T> T toInstance(ByteBuffer buffer) {
        Codec codec = codecTable[buffer.get()];
        buffer.getLong();// skip checksum
        int length = buffer.getInt();
        buffer.limit(buffer.position() + length);
        return (T) codec.toInstance(buffer.slice());
    }

    <T> T validateAndToInstance(ByteBuffer buffer) {
        Codec codec = codecTable[buffer.get()];
        long checksum = buffer.getLong();// TODO use unsign int
        int length = buffer.getInt();
        buffer.limit(buffer.position() + length);
        ByteBuffer body = buffer.slice();
        Checksums.validate(body.duplicate(), checksum);
        return (T) codec.toInstance(body);
    }

    private static class CodecWithCode {
        final Codec codec;
        final byte code;

        private CodecWithCode(byte code, Codec codec) {
            this.code = code;
            this.codec = codec;
        }
    }

}
