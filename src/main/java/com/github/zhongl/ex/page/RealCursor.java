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

package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;

import java.io.File;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class RealCursor<T> implements Cursor<T> {
    private final int offset;
    private final File file;
    private final Codec codec;

    RealCursor(File file, int offset, Codec codec) {
        this.file = file;
        this.offset = offset;
        this.codec = codec;
    }

    @Override
    public T get() {
        checkState(file.exists());
        ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file);
        buffer.position(offset);
        return codec.decode(buffer);
    }
}
