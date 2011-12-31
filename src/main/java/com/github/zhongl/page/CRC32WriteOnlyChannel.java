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

import com.google.common.primitives.Longs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.CRC32;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CRC32WriteOnlyChannel implements WritableByteChannel {

    private final CRC32 crc32 = new CRC32();
    private final FileChannel channel;

    public CRC32WriteOnlyChannel(File file) throws FileNotFoundException {
        channel = new FileOutputStream(file, true).getChannel();
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        updateCRC32By(buffer.duplicate());
        return channel.write(buffer);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        if (!channel.isOpen()) return;
        writeCRC32();
        channel.close();
    }

    private void writeCRC32() throws IOException {
        ByteBuffer crc32Buffer = ByteBuffer.wrap(Longs.toByteArray(crc32.getValue()));
        channel.write(crc32Buffer);
    }

    private void updateCRC32By(ByteBuffer buffer) {
        while (buffer.hasRemaining()) crc32.update(buffer.get());
    }
}
