package com.github.zhongl.ipage;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index extends Binder {
    public static final int ENTRY_LENGTH = Key.BYTE_LENGTH + 16;

    public Index(List<Page> pages) {
        super(pages);
    }

    public Range get(Key key) {
        if (pages.isEmpty()) return Range.NIL;
        return ((InnerPage) binarySearch(key)).get(key);
    }

    protected static abstract class InnerPage extends Page {
        protected final Keys keys;

        protected InnerPage(File file, Key key) {
            super(file, key);
            keys = new Keys();
        }

        public Range get(Key key) {
            try {
                ByteBuffer byteBuffer = positionedBufferByBinarySearch(key);
                return new Range(byteBuffer.getLong(), byteBuffer.getLong());
            } catch (IllegalStateException e) {
                return Range.NIL;
            }
        }

        protected ByteBuffer positionedBufferByBinarySearch(Key key) {
            int index = Collections.binarySearch(keys, key);
            checkState(index > -1);
            int position = index * ENTRY_LENGTH + Key.BYTE_LENGTH;
            return (ByteBuffer) buffer().position(position);
        }

        protected abstract ByteBuffer buffer();

        private class Keys extends AbstractList<Key> implements RandomAccess {

            @Override
            public Key get(int index) {
                ByteBuffer duplicate = buffer();
                byte[] bytes = new byte[Key.BYTE_LENGTH];
                duplicate.position(index * ENTRY_LENGTH);
                duplicate.get(bytes);
                return new Key(bytes);
            }

            @Override
            public int size() {
                return buffer().capacity() / ENTRY_LENGTH;
            }
        }
    }

}
