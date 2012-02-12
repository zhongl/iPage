package com.github.zhongl.ex.rvs;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
interface Migrater {

    void migrate(Key key, ByteBuffer buffer);
}
