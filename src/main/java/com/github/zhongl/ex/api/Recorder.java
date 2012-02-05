package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Recorder {

    /**
     * Append key and value, then notify {@link com.github.zhongl.ex.api.Browser}.
     *
     * @param key   {@link com.github.zhongl.ex.index.Md5Key}
     * @param value byte[]
     *
     * @return false if failed.
     */
    boolean append(Md5Key key, byte[] value);

    /**
     * Remove key and value by key, then notify {@link com.github.zhongl.ex.api.Browser}.
     *
     * @param key {@link com.github.zhongl.ex.index.Md5Key}
     *
     * @return false if failed.
     */
    boolean remove(Md5Key key);

}
