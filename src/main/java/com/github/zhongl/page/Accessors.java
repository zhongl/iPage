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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Accessors {
    public static final Accessor<String> STRING = new StringAccessor();


    private static class StringAccessor implements Accessor<String> {
        @Override
        public Writer writer(final String value) {
            return new LengthWriter() {
                @Override
                public int valueByteLength() {
                    return value.length();
                }

                @Override
                public int writeBodyTo(WritableByteChannel channel) throws IOException {
                    return channel.write(ByteBuffer.wrap(value.getBytes()));
                }
            };
        }

        @Override
        public Reader<String> reader() {
            return new LengthReader<String>() {
                @Override
                protected String readBodyFrom(ReadableByteChannel channel, int length) throws IOException {
                    byte[] bytes = new byte[length];
                    channel.read(ByteBuffer.wrap(bytes));
                    return new String(bytes);
                }
            };
        }

    }

}
