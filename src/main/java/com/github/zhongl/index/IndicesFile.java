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

import com.github.zhongl.codec.Encoder;
import com.github.zhongl.io.FileAppender;

import java.io.File;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class IndicesFile {

    private final FileAppender appender;
    private final Encoder<Index> encoder;

    IndicesFile(File dir, Encoder<Index> encoder) throws IOException {
        appender = new FileAppender(new File(dir, System.nanoTime() + ".i"));
        this.encoder = encoder;
    }

    public void append(Index index) throws IOException {
        appender.append(encoder.encode(index));
    }

    public File toFile() throws IOException {
        return appender.force();
    }
}
