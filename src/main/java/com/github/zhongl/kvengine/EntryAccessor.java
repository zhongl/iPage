/*
 * Copyright 2011 zhongl
 *
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

package com.github.zhongl.kvengine;

import com.github.zhongl.accessor.AbstractAccessor;
import com.github.zhongl.accessor.Accessor;
import com.github.zhongl.index.Md5Key;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
final class EntryAccessor<V> extends AbstractAccessor<Entry<V>> {

    private final Accessor<V> vAccessor;

    public EntryAccessor(Accessor<V> vAccessor) {this.vAccessor = vAccessor;}

    @Override
    protected void doWrite(Entry<V> entry, ByteBuffer buffer) {
        Md5Key.ACCESSOR.write(entry.key(), buffer);
        vAccessor.write(entry.value(), buffer);
    }

    @Override
    public int byteLengthOf(Entry<V> entry) {
        return Md5Key.ACCESSOR.byteLengthOf(entry.key()) + vAccessor.byteLengthOf(entry.value());
    }

    @Override
    public Entry<V> read(ByteBuffer buffer) {
        Md5Key key = Md5Key.ACCESSOR.read(buffer);
        V value = vAccessor.read(buffer);
        return new Entry<V>(key, value);
    }
}
