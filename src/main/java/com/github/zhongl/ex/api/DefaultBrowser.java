package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;

import java.util.Iterator;
import java.util.concurrent.Future;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DefaultBrowser extends Actor implements Browser, Updatable, Mergable {

    @Override
    public byte[] get(Md5Key key) {
        return new byte[0];  // TODO get
    }

    @Override
    public Iterator<byte[]> iterator() {
        return null;  // TODO iterator
    }

    @Override
    public void update(Future<Revision> future, Entry<Md5Key, byte[]> entry) {
        // TODO update
    }

    @Override
    public void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator) {
        // TODO merge
    }
}
