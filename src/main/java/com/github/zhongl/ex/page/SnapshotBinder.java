package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.util.Snapshot;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class SnapshotBinder<I> extends Binder<Object> implements Snapshot<I> {
    public SnapshotBinder(File dir, Codec codec) throws IOException {
        super(dir, codec);
    }

    @Override
    public SnapshotBinder merge(Iterator<I> iterator) throws IOException {
        if (isEmpty()) {
            merge(iterator, this);
            return this;
        }

        SnapshotBinder next = next();
        merge(iterator, next);
        remove();
        return next;
    }

    protected SnapshotBinder next() throws IOException {
        File parentFile = dir.getParentFile();
        String child = (Long.parseLong(dir.getName()) + 1) + "";
        File file = new File(parentFile, child);
        return next(file);
    }

    protected abstract void merge(Iterator<I> iterator, SnapshotBinder binder) throws IOException;

    protected abstract boolean isEmpty();

    protected abstract SnapshotBinder next(File dir) throws IOException;
}
