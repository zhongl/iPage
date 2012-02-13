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

package com.github.zhongl.ipage;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index extends Binder {
    public static final int ENTRY_LENGTH = Key.BYTE_LENGTH + 16;

    public Index(List<Page> pages) {
        super(pages);
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

        public Range get(Key key) {
            ByteBuffer byteBuffer = positionedBufferByBinarySearch(key);
            return new Range(byteBuffer.getLong(), byteBuffer.getLong());
        }

        protected ByteBuffer positionedBufferByBinarySearch(Key key) {
            int index = Collections.binarySearch(keys, key);
            if (index < 0) throw new IndexOutOfBoundsException(index + "");
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
