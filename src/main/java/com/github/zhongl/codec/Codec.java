package com.github.zhongl.codec;

import java.nio.ByteBuffer;

/**
 * A {@link Codec} implement should be a stateless for thread-safed.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public interface Codec {
    ByteBuffer encode(Object instance);

    <T> T decode(ByteBuffer buffer);

    boolean supports(Class<?> type);
}
