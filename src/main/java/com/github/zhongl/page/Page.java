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

package com.github.zhongl.page;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public abstract class Page<T> implements Iterable<T> {

    private static final WritableByteChannel CLOSED_CHANNEL = new WritableByteChannel() {
        @Override
        public int write(ByteBuffer src) throws IOException { return 0; }

        @Override
        public boolean isOpen() { return false; }

        @Override
        public void close() throws IOException { }
    };

    protected final File file;
    protected final Accessor<T> accessor;

    private final WritableByteChannel writeOnlychannel;
    private final long number;

    public Page(File file, Accessor<T> accessor) throws IOException {
        this.file = file;
        this.accessor = accessor;
        this.number = Long.parseLong(file.getName());
        this.writeOnlychannel = file.exists() ? CLOSED_CHANNEL : createWriteOnlyChannel(file);
    }

    protected abstract WritableByteChannel createWriteOnlyChannel(File file) throws FileNotFoundException;

    public int add(T object) throws IOException {
        checkState(writeOnlychannel.isOpen(), "Fixed page can't add %s", object);
        return accessor.writer(object).writeTo(writeOnlychannel);
    }

    public void fix() throws IOException {
        if (writeOnlychannel.isOpen()) writeOnlychannel.close();
    }

    @Override
    public abstract Iterator<T> iterator();

    public void clear() {
        checkState(file.delete(), "Can't delete page %s", file);
    }

    public long number() {
        return number;
    }
}
