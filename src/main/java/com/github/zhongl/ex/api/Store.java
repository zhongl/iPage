package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
interface Store extends Iterable<byte[]> {
    void append(Revision revision, Entry<Md5Key, byte[]> entry);

    void remove(Revision revision, Offset offset);
}
