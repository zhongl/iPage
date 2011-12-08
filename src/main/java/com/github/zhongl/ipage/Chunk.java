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

package com.github.zhongl.ipage;

import com.github.zhongl.accessor.Accessor;
import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;
import com.google.common.collect.AbstractIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.github.zhongl.util.ByteBuffers.slice;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link com.github.zhongl.ipage.Chunk}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class Chunk<T> extends MappedFile implements Iterable<T>, Closeable, ValidateOrRecover<T, IOException> {

    public static final int DEFAULT_CAPACITY = 4096; // 4k
    private final Accessor<T> accessor;
    private final long beginPositionInIPage;

    private volatile int writePosition = 0;
    private volatile boolean erased;

    public Chunk(long beginPositionInIPage, File file, long capacity, Accessor<T> accessor) throws IOException {
        super(file, capacity);
        this.beginPositionInIPage = beginPositionInIPage;
        this.accessor = accessor;
        this.writePosition = (int) file.length();
    }

    public long append(T record) throws IOException {
        checkState(!erased, "Chunk %s has already erased", file);
        checkOverFlowIfAppend(record);
        long iPageOffset = writePosition + beginPositionInIPage;
        ensureMap();
        writePosition += accessor.write(record, slice(mappedByteBuffer, writePosition));  // TODO use a better name to instead "slice"
        return iPageOffset;
    }

    public T get(long offset) throws IOException {
        checkState(!erased, "Chunk %s has already erased", file);
        try {
            return getInternal((int) (offset - beginPositionInIPage));
        } catch (RuntimeException e) {
            return null;
        }
    }

    @Override
    public boolean flush() throws IOException {
        return !erased && super.flush();
    }

    public long endPositionInIPage() {
        checkState(!erased, "Chunk %s has already erased", file);
        return beginPositionInIPage + writePosition - 1;
    }

    public long beginPositionInIPage() {
        checkState(!erased, "Chunk %s has already erased", file);
        return beginPositionInIPage;
    }

    @Override
    @Deprecated
    public Iterator<T> iterator() {
        checkState(!erased, "Chunk %s has already erased", file);
        return new RecordIterator(writePosition);
    }

    public void erase() throws IOException {
        if (erased) return;
        erased = true;
        releaseBuffer();
        deleteFile();
    }

    @Override
    public void close() throws IOException {
        if (flush() && releaseBuffer()) setLength(writePosition);
    }

    @Override
    public boolean validateOrRecoverBy(Validator<T, IOException> validator) throws IOException {
        int offset = 0;
        while (offset < writePosition) {
            T object = getInternal(offset);
            if (!validator.validate(object)) {
                writePosition = offset;
                return false;
            }
            offset += accessor.byteLengthOf(object);
        }
        return true;
    }

    private T getInternal(int offset) throws IOException {
        ensureMap();
        return accessor.read(slice(mappedByteBuffer, offset, writePosition - offset));
    }

    private void checkOverFlowIfAppend(T record) {
        int appendedPosition = writePosition + accessor.byteLengthOf(record);
        if (appendedPosition > capacity) throw new OverflowException();
    }

    @Deprecated
    private class RecordIterator extends AbstractIterator<T> {
        private int offset;
        private final int limit;

        private RecordIterator(int limit) {
            this.limit = limit;
        }

        @Override
        protected T computeNext() {
            try {
                // TODO Slove concurrent modification problem
                checkState(!erased, "Chunk %s has already erased", file);
                if (offset >= limit) return endOfData();
                T record = getInternal(offset);
                offset += accessor.byteLengthOf(record);
                return record;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (RuntimeException e) { // read the appending chunk may cause the exception.
                return endOfData();
            }
        }
    }

}
