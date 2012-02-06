package com.github.zhongl.ex.page;

import com.google.common.util.concurrent.FutureCallback;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Appendable {

    void append(Object value, FutureCallback<Cursor> callback);

    void force();
}
