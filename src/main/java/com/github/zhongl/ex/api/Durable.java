package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Asynchronize;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Checkpoint;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.util.Entry;
import com.google.common.util.concurrent.FutureCallback;

import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@Asynchronize
public interface Durable {
    void merge(List<Entry<Md5Key, Cursor>> removings, List<Entry<Md5Key, byte[]>> appendings, Checkpoint checkpoint);

    void get(Cursor cursor, FutureCallback<byte[]> callback);

}
