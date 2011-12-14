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

import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class IPage<T> implements Closeable, ValidateOrRecover<T, IOException> {

    private final GarbageCollector<T> garbageCollector;
    private final ChunkList<T> chunkList;

    public static <T> Builder<T> baseOn(File dir) {
        return new Builder<T>(dir);
    }

    IPage(ChunkList<T> chunkList) {
        this.chunkList = chunkList;
        garbageCollector = new GarbageCollector<T>();
    }

    public long append(T record) throws IOException {
        try {
            return chunkList.last().append(record);
        } catch (BufferOverflowException e) {
            chunkList.grow();
            return append(record);
        }
    }

    public T get(long offset) throws IOException {
        try {
            return chunkList.chunkIn(offset).get(offset);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    public Cursor<T> next(Cursor<T> cursor) throws IOException {
        try {
            long beginPosition = chunkList.first().beginPosition();
            if (cursor.offset() < beginPosition) cursor = Cursor.begin(beginPosition);
            return chunkList.chunkIn(cursor.offset()).next(cursor);
        } catch (IndexOutOfBoundsException e) {
            return cursor.end();
        }
    }

    public long garbageCollect(long survivorOffset) throws IOException {
        return garbageCollector.collect(survivorOffset, chunkList);
    }

    public void flush() throws IOException {
        chunkList.last().flush();
    }

    @Override
    public boolean validateOrRecoverBy(Validator<T, IOException> validator) throws IOException {
        return chunkList.last().validateOrRecoverBy(validator);
    }

    @Override
    public void close() throws IOException {
        chunkList.close();
    }

}
