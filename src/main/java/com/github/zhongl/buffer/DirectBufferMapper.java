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

package com.github.zhongl.buffer;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class DirectBufferMapper {
    private final File file;
    private final int capacity;
    private final boolean readOnly;

    public DirectBufferMapper(File file, int capacity, boolean readOnly) {
        this.file = file;
        this.capacity = capacity;
        this.readOnly = readOnly;
    }

    public MappedByteBuffer map() throws IOException {
        return Files.map(file, readOnly ? READ_ONLY : READ_WRITE, capacity);
    }

    public int capacity() { return capacity; }

    public long maxIdleTimeMillis() {
        return 0;  // TODO maxIdleTimeMillis
    }
}
