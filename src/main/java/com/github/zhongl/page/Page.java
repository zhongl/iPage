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

package com.github.zhongl.page;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public abstract class Page<T> implements Iterable<T> {
    public static final int CRC32_LENGTH = 8;

    protected final File file;
    protected final Accessor<T> accessor;

    private final WritableByteChannel writeOnlychannel;

    public Page(File file, Accessor<T> accessor) throws IOException {
        this.file = file;
        this.accessor = accessor;

        if (file.exists()) {
            writeOnlychannel = null;
            validateCheckSum(file);
        } else
            this.writeOnlychannel = new CRC32WriteOnlyChannel(file);
    }

    private void validateCheckSum(File file) throws IOException {
        long offset = file.length() - CRC32_LENGTH;
        FileInputStream stream = new FileInputStream(file);
        try {
            checkState(offset >= 0 && validateCheckSum(stream, offset));
        } finally {
            stream.close();
        }
    }

    public int add(T object) throws IOException {
        checkState(writeOnlychannel != null && writeOnlychannel.isOpen(), "Fixed page can't add %s", object);
        return accessor.writer(object).writeTo(writeOnlychannel);
    }


    public void fix() throws IOException {
        if (writeOnlychannel == null || !writeOnlychannel.isOpen()) return;
        writeOnlychannel.close();
    }

    @Override
    public abstract Iterator<T> iterator();

    public void clear() {
        checkState(file.delete(), "Can't delete page %s", file);
    }

    private static boolean validateCheckSum(FileInputStream fileInputStream, long offset) throws IOException {
        DataInputStream stream = new DataInputStream(new BufferedInputStream(fileInputStream));
        CRC32 crc32 = new CRC32();
        for (long i = 0; i < offset; i++) {
            crc32.update(stream.read());
        }
        return crc32.getValue() == stream.readLong();
    }
}
