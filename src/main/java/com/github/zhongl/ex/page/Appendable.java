package com.github.zhongl.ex.page;

import com.google.common.util.concurrent.FutureCallback;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Appendable<V> {

    void append(V value, FutureCallback<Cursor> callback);

    void force();
}
