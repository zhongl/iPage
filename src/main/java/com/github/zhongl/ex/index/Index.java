package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Index implements Closable {

    static final int CAPACITY = Integer.getInteger("ipage.index.page.capacity", 4096);

    private InnerBinder binder;
    private Entry<Md5Key, Offset> currentAppendingEntry;

    public Index() {
    }

    public void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator) {
        // TODO merge
    }

    public Offset get(Md5Key key) {
        return binder.get(key);  // TODO get
    }

    @Override
    public void close() {
        // TODO close
    }

    private class InnerBinder extends Binder {
        protected InnerBinder(File dir, Codec codec) throws IOException {
            super(dir, codec);
        }

        @Override
        public <T> Cursor append(T value, boolean force) throws IOException {
            currentAppendingEntry = (Entry<Md5Key, Offset>) value;
            return super.append(value, force);    // TODO append
        }

        @Override
        protected Page newPage(File file, Number number, Codec codec) {
            return new InnerPage(file, number, CAPACITY, codec);
        }

        @Override
        protected Number newNumber(@Nullable Page last) {
            return currentAppendingEntry.key();
        }

        @Override
        protected Number parseNumber(String text) {
            return new Md5Key(text);
        }

        public Offset get(Md5Key key) {
            int index = binarySearchPageIndex(key);
            if (index < 0) return null;
            return ((InnerPage) pages.get(index)).get(key);
        }
    }

    private class InnerPage extends Page {
        protected InnerPage(File file, Number number, int capacity, Codec codec) {
            super(file, number, capacity, codec);
        }

        @Override
        protected Batch newBatch(CursorFactory cursorFactory, int position, int estimateBufferSize) {
            return null;  // TODO newBatch
        }

        public Offset get(Md5Key key) {
            return null;  // TODO get
        }
    }
}
