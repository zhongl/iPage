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

    static final int HEAD_LENGTH = 1/*codec_code*/ + 8 /*checksum*/ + 4 /*body_length*/;

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

    ByteBuffer encode(Object instance) {
        CodecWithCode codecWithCode = codecMap.get(instance.getClass());
        if (codecWithCode == null)
            throw new IllegalStateException("Can't find the codec of " + instance);

        ByteBuffer body = codecWithCode.codec.encode(instance);
        ByteBuffer encoded = ByteBuffer.allocate(HEAD_LENGTH + body.limit());
        encoded.put(codecWithCode.code)
                .putLong(Checksums.checksum(body.duplicate())) // TODO use unsign int for checksum
                .putInt(body.limit()) // TODO use varint for length
                .put(body) // body
                .flip();
        return encoded;
    }

    <T> T decode(ByteBuffer buffer) {
        return new Decoder(buffer).getValue();
    }

    <T> T decodeAndValidate(ByteBuffer buffer) {
        Decoder decoder = new Decoder(buffer);
        decoder.validate();
        return decoder.getValue();
    }

    private class Decoder {

        private final ByteBuffer body;
        private final long checksum;
        private final byte codecCode;

        public Decoder(ByteBuffer buffer) {
            codecCode = buffer.get();
            checksum = buffer.getLong();
            int length = buffer.getInt();
            buffer.limit(buffer.position() + length);
            body = buffer.slice();
        }

        public <T> T getValue() {
            return (T) codecTable[codecCode].decode(body);
        }

        public void validate() {
            Checksums.validate(body.duplicate(), checksum);
        }
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
