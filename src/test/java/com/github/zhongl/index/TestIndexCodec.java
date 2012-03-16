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

package com.github.zhongl.index;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class TestIndexCodec implements IndexCodec {
    @Override
    public ByteBuffer encode(Index value) {
        return (ByteBuffer) ByteBuffer.allocate(4).putInt(((TestKey) value.key()).value).flip();
    }

    @Override
    public int encode(Index value, ByteBuffer byteBuffer) {
        byteBuffer.putInt(((TestKey) value.key()).value);
        return length();
    }

    @Override
    public Index decode(ByteBuffer byteBuffer) { return new TestIndex(byteBuffer.getInt(), false); }

    @Override
    public int length() { return 4; }
}
