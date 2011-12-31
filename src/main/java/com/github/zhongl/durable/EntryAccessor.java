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

package com.github.zhongl.durable;

import com.github.zhongl.index.Md5Key;
import com.github.zhongl.page.Accessor;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
final class EntryAccessor<V> implements Accessor<Entry<V>> {

    private final Accessor<V> vAccessor;

    public EntryAccessor(Accessor<V> vAccessor) {this.vAccessor = vAccessor;}

    @Override
    public Writer writer(Entry<V> value) {
        final Writer kWriter = Md5Key.ACCESSOR.writer(value.key());
        final Writer vWriter = vAccessor.writer(value.value());
        return new Writer() {

            @Override
            public int valueByteLength() {
                return kWriter.valueByteLength() + vWriter.valueByteLength();
            }

            @Override
            public int writeTo(WritableByteChannel channel) throws IOException {
                return kWriter.writeTo(channel) + vWriter.writeTo(channel);
            }
        };
    }

    @Override
    public Reader<Entry<V>> reader() {
        final Reader<Md5Key> kReader = Md5Key.ACCESSOR.reader();
        final Reader<V> vReader = vAccessor.reader();
        return new Reader<Entry<V>>() {

            @Override
            public Entry<V> readFrom(ReadableByteChannel channel) throws IOException {
                Md5Key md5Key = kReader.readFrom(channel);
                V value = vReader.readFrom(channel);
                return new Entry<V>(md5Key, value);
            }
        };
    }
}
