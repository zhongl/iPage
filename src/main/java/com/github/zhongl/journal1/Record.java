package com.github.zhongl.journal1;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Record {
    Record EOF = new Record() {
        @Override
        public long offset() { return Long.MAX_VALUE; }

        @Override
        public int length() { return 0; }

        @Override
        public <T> T content() { return null; }

        @Override
        public ByteBuffer contentBuffer() { return ByteBuffer.allocate(0); }
    };

    long offset();

    int length();

    <T> T content();

    ByteBuffer contentBuffer();
}
