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


import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closeable {

    private volatile Page first;
    private volatile Page last;

    public Journal(Page first, Page last) {
        this.first = first;
        this.last = last;
    }

    public static Journal open(File dir) {

        Page tail = null;
        Page head = null;
        return new Journal(head, tail);
    }

    public void append(ByteBuffer buffer) throws IOException {
        last = last.append(buffer);
    }

    @Override
    public void close() throws IOException {
        // TODO close
    }

    public void applyTo(ByteBufferHandler handler) throws Exception {
        handler.handle(first.head().get());
        first = first.remove();
        last = last.saveCheckpoint(first.head());
    }
}
