/*
 * Copyright 2012 zhongl
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.index;

import com.github.zhongl.page.Binder;
import com.github.zhongl.page.Page;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index extends Binder {
    public static final int ENTRY_LENGTH = Key.BYTE_LENGTH + 16;

    public Index(List<Page> pages) { super(pages); }

    protected static Key getKey(ByteBuffer buffer, int position) {
        byte[] bytes = new byte[Key.BYTE_LENGTH];
        ((ByteBuffer) buffer.position(position)).get(bytes);
        return new Key(bytes);
    }

    public Range get(Key key) {
        if (pages.isEmpty()) return Range.NIL;
        try {
            return ((InnerPage) binarySearch(key)).get(key);
        } catch (IndexOutOfBoundsException e) {
            return Range.NIL;
        }
    }

    protected static abstract class InnerPage extends Page {

        protected final Keys keys;

        protected InnerPage(File file, Key key) {
            super(file, key);
            keys = new Keys();
        }

        protected int positionOf(Key key) {
            int index = Collections.binarySearch(keys, key);
            if (index < 0) throw new IndexOutOfBoundsException(index + "");
            return index * ENTRY_LENGTH + Key.BYTE_LENGTH;
        }

        public Range get(Key key) {
            ByteBuffer duplicate = buffer();
            duplicate.position(positionOf(key));
            return new Range(duplicate.getLong(), duplicate.getLong());
        }

        protected abstract ByteBuffer buffer();

        protected class Keys extends AbstractList<Key> implements RandomAccess {
            @Override
            public Key get(int index) { return getKey(buffer(), index * ENTRY_LENGTH); }

            @Override
            public int size() { return buffer().capacity() / ENTRY_LENGTH; }
        }

    }

}
