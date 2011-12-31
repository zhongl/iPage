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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessor;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Cursor implements Comparable<Cursor> {
    public static final Accessor<Cursor> ACCESSOR = new InnerAccessor();
    public static final Cursor NULL = null;

    final long offset;

    public static Cursor head() {
        return new Cursor(0L);
    }

    public static Cursor valueOf(String value) {
        return new Cursor(Long.parseLong(value));
    }

    public Cursor(long offset) {
        checkArgument(offset >= 0);
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Cursor)) return false;
        if (offset != ((Cursor) o).offset) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return (int) (offset ^ (offset >>> 32));
    }

    @Override
    public String toString() {
        return Long.toString(offset);
    }

    @Override
    public int compareTo(Cursor that) {
        return (int) (this.offset - that.offset);
    }

    public Cursor forword(int length) {
        return new Cursor(offset + length);
    }

    public int indexIn(List<? extends Comparable<Cursor>> list) {
        int low = 0, high = list.size() - 1;
        while (low <= high) { // binary search
            int mid = (low + high) >>> 1;
            int cmp = list.get(mid).compareTo(this);
            if (cmp < 0) low = mid + 1;
            else if (cmp > 0) high = mid - 1;
            else return mid;
        }
        return -(low + 1);
    }

    public int distanceTo(Cursor that) {
        return Math.abs(compareTo(that));
    }

    private static class InnerAccessor implements Accessor<Cursor> {
        @Override
        public Writer writer(final Cursor value) {
            return new Writer() {
                @Override
                public int valueByteLength() {
                    return 8;
                }

                @Override
                public int writeTo(WritableByteChannel channel) throws IOException {
                    return channel.write(ByteBuffer.allocate(8).putLong(0, value.offset));
                }
            };
        }

        @Override
        public Accessor.Reader<Cursor> reader() {
            return new Reader<Cursor>() {
                @Override
                public Cursor readFrom(ReadableByteChannel channel) throws IOException {
                    ByteBuffer buffer = ByteBuffer.allocate(8);
                    channel.read(buffer);
                    return new Cursor(buffer.getLong(0));
                }
            };
        }
    }


}
