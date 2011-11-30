package com.github.zhongl.ipage;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class ChunkContentUtils {
    private ChunkContentUtils() { }

    public static byte[] concatToChunkContentWith(byte[]... data) {
        int lengthBytes = 4;
        int length = 0;

        for (byte[] bytes : data) {
            length += bytes.length + lengthBytes;
        }

        byte[] union = new byte[length];

        ByteBuffer buffer = ByteBuffer.wrap(union);

        for (byte[] item : data) {
            buffer.putInt(item.length);
            buffer.put(item);
        }
        return union;
    }
}
