package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Cursor;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Reader {
    public abstract byte[] get(Md5Key key);

    abstract void update(Revision revision, Entry<Md5Key, byte[]> entry);

    abstract void merge(Entry<Md5Key, Cursor> entry, boolean force);
}
