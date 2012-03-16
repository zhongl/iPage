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

import com.github.zhongl.index.Key;
import com.github.zhongl.index.KeyCodec;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Md5KeyCodec implements KeyCodec {
    @Override
    public Md5Key decode(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[length()];
        byteBuffer.get(bytes);
        return new Md5Key(bytes);
    }

    @Override
    public ByteBuffer encode(Key value) {
        return (ByteBuffer) ByteBuffer.allocate(length()).put(((Md5Key) value).toBytes()).flip();
    }

    @Override
    public int encode(Key value, ByteBuffer byteBuffer) {
        byteBuffer.put(((Md5Key) value).toBytes());
        return length();
    }

    @Override
    public int length() {
        return Md5Key.BYTE_LENGTH;
    }
}
