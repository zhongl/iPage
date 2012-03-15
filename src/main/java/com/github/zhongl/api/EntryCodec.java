/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.api;

import com.github.zhongl.codec.Codec;
import com.github.zhongl.index.Key;
import com.github.zhongl.index.KeyCodec;
import com.github.zhongl.util.Entry;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class EntryCodec<V> implements Codec<Entry<Key, V>> {
    private final KeyCodec keyCodec;
    private final Codec<V> valueCodec;

    EntryCodec(KeyCodec keyCodec, Codec<V> valueCodec) {
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    @Override
    public Entry<Key, V> decode(ByteBuffer byteBuffer) {
        Key key = keyCodec.decode(byteBuffer);
        int length = byteBuffer.getInt();
        byteBuffer.limit(byteBuffer.position() + length);
        V value = valueCodec.decode(byteBuffer);
        return new Entry<Key, V>(key, value);
    }

    @Override
    public ByteBuffer encode(Entry<Key, V> entry) {
        ByteBuffer kBuffer = keyCodec.encode(entry.key());
        ByteBuffer vBuffer = valueCodec.encode(entry.value());
        return (ByteBuffer) ByteBuffer.allocate(kBuffer.remaining() + 4 + vBuffer.remaining())
                                      .put(kBuffer)
                                      .putInt(vBuffer.remaining())
                                      .put(vBuffer)
                                      .flip();
    }
}
