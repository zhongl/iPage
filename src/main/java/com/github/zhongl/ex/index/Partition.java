package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;
import com.github.zhongl.ex.util.Entry;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.RandomAccess;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class Partition extends Page implements Iterable<Entry<Md5Key, Cursor>> {
    private final Entries entries = new Entries();
    private final Keys keys = new Keys();

    protected Partition(File file, Number number, Codec codec) {
        super(file, number, codec);
    }

    @Override
    protected Batch newBatch(int estimateBufferSize) {
        return new DefaultBatch(codec(), file().length(), estimateBufferSize);
    }

    public Cursor get(Md5Key key) {
        if (!file().exists()) return null;
        int index = Collections.binarySearch(keys, key);
        if (index < 0) return null;
        return entries.get(index).value();
    }

    @Override
    public Iterator<Entry<Md5Key, Cursor>> iterator() {
        return entries.iterator();
    }

    private abstract class RandomAccessList<T> extends AbstractList<T> implements RandomAccess {
        @Override
        public int size() {
            if (file().length() == 0) return 0;
            return ReadOnlyMappedBuffers.getOrMap(file()).capacity() / EntryCodec.LENGTH;
        }

        @Override
        public T get(int index) {
            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file());
            buffer.position(index * EntryCodec.LENGTH).limit((index + 1) * EntryCodec.LENGTH);
            return decode(buffer);
        }

        protected abstract T decode(ByteBuffer buffer);
    }

    private class Keys extends RandomAccessList<Md5Key> {

        @Override
        protected Md5Key decode(ByteBuffer buffer) {
            byte[] bytes = new byte[Md5Key.BYTE_LENGTH];
            buffer.get(bytes);
            return new Md5Key(bytes);
        }

    }

    private class Entries extends RandomAccessList<Entry<Md5Key, Cursor>> {
        @Override
        protected Entry<Md5Key, Cursor> decode(ByteBuffer buffer) {return codec().decode(buffer);}
    }
}
