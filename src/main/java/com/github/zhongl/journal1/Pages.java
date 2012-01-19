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

package com.github.zhongl.journal1;

import com.github.zhongl.codec.Codec;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Pages implements Closeable {

    private final Codec codec;

    public Pages(File dir, Codec codec) {
        // TODO Pages
        this.codec = codec;
    }

    /**
     * Append any object which the {@link com.github.zhongl.codec.Codec} supported.
     *
     * @param object
     * @param force
     *
     * @return appended position.
     */
    public long append(Object object, boolean force) {
        // TODO append
        return 0L;
    }

    @Override
    public void close() throws IOException {
        // TODO close
    }

    public void trimBefore(long position) {
        // TODO trimBefore
    }

    public Cursor head() {
        return null;  // TODO head
    }

    public Cursor next(Cursor cursor) {
        return null;  // TODO next 
    }

    public void reset() {
        // TODO reset
    }

    public void trimAfter(long position) {
        // TODO trimAfter
    }
}
