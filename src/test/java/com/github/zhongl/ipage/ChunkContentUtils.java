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

package com.github.zhongl.ipage;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class ChunkContentUtils {
    private ChunkContentUtils() { }

    public static byte[] concatToChunkContentWith(byte[]... data) {
        int lengthBytes = 4;
        int length = 0;

        for (byte[] bytes : data) {
            length += bytes.length + lengthBytes;
        }

        byte[] union = new byte[length];

        ByteBuffer buffer = ByteBuffer.wrap(union);

        for (byte[] item : data) {
            buffer.putInt(item.length);
            buffer.put(item);
        }
        return union;
    }
}
