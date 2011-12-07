package com.github.zhongl.ipage;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface ByteBufferAccessor<T> {
    /**
     * Byte length of the object store in {@link java.nio.ByteBuffer}.
     *
     * @param object T
     *
     * @return length
     */
    int lengthOf(T object);

    Writer write(T object);

    Reader<T> read(ByteBuffer buffer);

    interface Reader<T> {
        /**
         * Get object from {@link java.nio.ByteBuffer}.
         *
         * @return instance of T
         */
        public T get();
    }

    interface Writer {
        /**
         * Write object to {@link java.nio.ByteBuffer}.
         *
         * @param buffer {@link java.nio.ByteBuffer}
         *
         * @return wrote length.
         */
        public int to(ByteBuffer buffer);
    }
}
