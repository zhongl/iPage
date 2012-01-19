package com.github.zhongl.journal1;

import java.nio.ByteBuffer;

/**
 * A {@link com.github.zhongl.journal1.Codec} implement should be a stateless for thread-safed.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public interface Codec {
    ByteBuffer toBuffer(Object instance);

    Object toInstance(ByteBuffer buffer);

    Class<?> supported();
}
