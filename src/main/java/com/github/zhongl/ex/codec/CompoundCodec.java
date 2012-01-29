package com.github.zhongl.ex.codec;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CompoundCodec implements Codec {

    static final int CODE_LENGTH = 1/*codec_code*/ ;

    private final Map<Class<?>, CodecWithCode> codecMap;
    private final Codec[] codecTable;

    public CompoundCodec(Codec... codecs) {
        checkArgument(codecs.length < Byte.MAX_VALUE, "Less than %s codec can be register.", Byte.MAX_VALUE);
        this.codecTable = codecs;
        codecMap = Collections.synchronizedMap(new HashMap<Class<?>, CodecWithCode>());
    }

    public ByteBuffer encode(Object instance) {
        CodecWithCode codecWithCode = codecMap.get(instance.getClass());
        if (codecWithCode == null)
            codecWithCode = findInCodecTable(instance.getClass());

        if (codecWithCode == null)
            throw new IllegalStateException("Can't find the codec of " + instance);

        ByteBuffer body = codecWithCode.codec.encode(instance);
        return encode(codecWithCode.code, body);
    }

    private CodecWithCode findInCodecTable(Class<?> type) {
        for (int i = 0; i < codecTable.length; i++) {
            if (codecTable[i].supports(type)) {
                CodecWithCode codecWithCode = new CodecWithCode((byte) i, codecTable[i]);
                codecMap.put(type, codecWithCode);
                return codecWithCode;
            }
        }
        return null;
    }

    private ByteBuffer encode(byte code, ByteBuffer body) {
        int length = ByteBuffers.lengthOf(body);
        ByteBuffer encoded = ByteBuffer.allocate(CODE_LENGTH + length);
        encoded.put(code).put(body).flip();
        return encoded;
    }

    @Override
    public <T> T decode(ByteBuffer buffer) {
        return codecTable[buffer.get()].decode(buffer);
    }

    @Override
    public boolean supports(Class<?> type) {
        for (Codec codec : codecTable) {
            if (!codec.supports(type)) return false;
        }
        return true;
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
