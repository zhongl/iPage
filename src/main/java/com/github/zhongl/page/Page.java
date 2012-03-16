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

import com.github.zhongl.codec.Decoder;
import com.github.zhongl.io.FileAppender;
import com.github.zhongl.io.FileChannels;
import com.github.zhongl.io.IterableFile;
import com.github.zhongl.util.Nils;
import com.google.common.base.Function;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Page<V> extends Numbered<Offset> implements Iterable<Element<V>> {
    protected final File file;
    protected final Decoder<V> decoder;

    public Page(File file, Offset offset, Decoder<V> decoder) {
        super(offset);
        this.file = file;
        this.decoder = decoder;
    }

    public String fileName() {return file.getName();}

    public V get(final Range range) {
        try {
            return FileChannels.read(file, refer(range.from()), (int) range.length(), new Function<ByteBuffer, V>() {
                @Override
                public V apply(ByteBuffer byteBuffer) {
                    return decoder.decode(byteBuffer);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public Offset nextPageNumber() { return new Offset(number().value() + file.length()); }

    @Override
    public Iterator<Element<V>> iterator() {
        return new IterableFile(file).toIterator(new Function<ByteBuffer, Element<V>>() {
            private long position = number().value();

            @Override
            public Element<V> apply(ByteBuffer byteBuffer) {
                int last = byteBuffer.position();
                V value = decoder.decode(byteBuffer);
                int length = byteBuffer.position() - last;
                Element<V> element = new Element<V>(value, new Range(position, position + length));
                position += length;
                return element;
            }

        });
    }

    public void transferTo(final FileAppender fileAppender, final RangeJoiner joiner) {
        try {
            FileChannels.read(file, new FileChannels.FileChannelFunction<Void>() {

                @Override
                public Void apply(FileChannel channel) throws IOException {
                    for (Range range : joiner) {
                        fileAppender.transferFrom(channel, refer(range.from()), (int) (range.length()));
                    }
                    return Nils.VOID;
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private int refer(long absolute) { return (int) (absolute - number().value()); }

}
