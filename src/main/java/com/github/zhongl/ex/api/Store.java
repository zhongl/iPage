package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Asynchronize;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;
import com.google.common.util.concurrent.FutureCallback;

import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@Asynchronize
interface Store {
    void append(Revision revision, Entry<Md5Key, byte[]> entry);

    void remove(Revision revision, Offset offset);

    void get(Offset offset, FutureCallback<byte[]> callback);

    void iterator(FutureCallback<Iterator<byte[]>> callback);
}
