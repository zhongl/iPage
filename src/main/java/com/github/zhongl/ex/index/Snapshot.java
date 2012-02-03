package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.nio.Transferable;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;
import com.google.common.collect.AbstractIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Snapshot extends Binder implements Iterable<Entry<Md5Key, Offset>> {
    private static final int CAPACITY = FlexIndex.MAX_ENTRY_SIZE * EntryCodec.LENGTH;

    private Entry<Md5Key, Offset> currentAppendingEntry;

    Snapshot(File dir, Codec codec) throws IOException { super(dir, codec); }

    @Override
    public <T> Cursor append(T value, boolean force) throws IOException {
        currentAppendingEntry = (Entry<Md5Key, Offset>) value;
        return super.append(value, force);
    }

    @Override
    protected Page newPage(File file, Number number, Codec codec) {
        return new Partition(file, number, CAPACITY, codec);
    }

    @Override
    protected Number newNumber(@Nullable Page last) {
        return currentAppendingEntry == null ? Md5Key.MIN : currentAppendingEntry.key();
    }

    @Override
    protected Number parseNumber(String text) {
        return new Md5Key(text);
    }

    public Offset get(Md5Key key) {
        return ((Partition) pages.get(binarySearchPageIndex(key))).get(key);// index will always in [0, pages.size)
    }

    public void remove() {
        while (!pages.isEmpty()) removeHeadPage();
        checkState(dir.delete());
    }

    public Snapshot newSnapshot() throws IOException {
        File parentFile = dir.getParentFile();
        String file = (Long.parseLong(dir.getName()) + 1) + "";
        return new Snapshot(new File(parentFile, file), codec);
    }

    @Override
    public Iterator<Entry<Md5Key, Offset>> iterator() {
        return new EntryIterator();
    }

    class EntryIterator extends AbstractIterator<Entry<Md5Key, Offset>> {
        private int index = 0;
        private int offset = 0;

        @Override
        protected Entry<Md5Key, Offset> computeNext() {
            if (index == pages.size()) return endOfData();

            File file = pages.get(index).file();
            if (file.length() == 0) return endOfData();

            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file);
            buffer.position(offset);
            Entry<Md5Key, Offset> entry = codec.decode(buffer);

            offset += EntryCodec.LENGTH;
            if (offset == file.length()) {
                index += 1;
                offset = 0;
            }
            return entry;
        }

        public void transferTo(Transferable transferable) throws IOException {
            for (int i = index; i < pages.size(); i++) {
                File file = pages.get(i).file();
                transferable.transferFrom(file, (i == index) ? offset : 0L);
            }
        }

    }


}
