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

package com.github.zhongl.index;

import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;
import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.io.Files;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static com.github.zhongl.util.ByteBuffers.slice;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link FileHashTable} is a file-based hash map for mapping
 * {@link Md5Key} and offset of {@link com.github.zhongl.kvengine.Entry} in
 * {@link com.github.zhongl.ipage.IPage}.
 * <p/>
 * It is a implemente of separate chain hash table, more infomation you can find in "Data Structures & Algorithms In Java".
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public final class FileHashTable implements ValidateOrRecover<Slot, IOException> {

    public static final Long NULL_OFFSET = null;
    public static final int DEFAULT_SIZE = 256;

    private final MappedByteBuffer mappedByteBuffer;
    private final int amountOfBuckets;
    private final File file;

    private volatile int occupiedSlots;
    private volatile boolean cleaned;
    private volatile boolean closed;

    public FileHashTable(File file, int buckets) throws IOException {
        this.file = file;
        amountOfBuckets = buckets > 0 ? buckets : DEFAULT_SIZE;
        mappedByteBuffer = Files.map(file, READ_WRITE, amountOfBuckets * Bucket.LENGTH);
        calculateOccupiedSlots();
    }

    public int amountOfBuckets() {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        return amountOfBuckets;
    }

    public int size() {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        return occupiedSlots;
    }

    public Long put(Md5Key key, Long offset) {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        Bucket bucket = buckets(hashAndMod(key));
        Long preoffset = bucket.put(key, offset);
        bucket.updateCRC();
        if (preoffset == null) occupiedSlots++; // add a new key
        return preoffset;
    }

    public Long get(Md5Key key) {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        return buckets(hashAndMod(key)).get(key);
    }

    public Long remove(Md5Key key) {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        Bucket bucket = buckets(hashAndMod(key));
        Long preoffset = bucket.remove(key);
        if (preoffset != null) { // remove an exist key
            bucket.updateCRC();
            occupiedSlots--;
        }
        return preoffset;
    }

    public void close() { // TODO remve Closeable because of IOException
        if (cleaned || closed) return;
        flush();
        DirectByteBufferCleaner.clean(mappedByteBuffer);
        closed = true;
    }

    private int hashAndMod(Md5Key key) {
        return Math.abs(key.hashCode()) % amountOfBuckets;
    }

    private void calculateOccupiedSlots() {
        for (int i = 0; i < amountOfBuckets; i++) {
            occupiedSlots += buckets(i).occupiedSlots();
        }
    }

    private Bucket buckets(int i) {
        return new Bucket(slice(mappedByteBuffer, i * Bucket.LENGTH, Bucket.LENGTH));
    }

    public void flush() {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        mappedByteBuffer.force();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void clean() {
        if (cleaned) return;
        cleaned = true;
        checkState(file.delete(), "Can't delete file %s", file);
    }

    public File file() {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        return file;
    }

    @Override
    public boolean validateOrRecoverBy(Validator<Slot, IOException> validator) throws IOException {
        checkState(!cleaned && !closed, "FileHashTable %s has already cleaned or closed.", file);
        for (int i = 0; i < amountOfBuckets; i++) {
            Bucket bucket = buckets(i);
            if (bucket.checkCRC()) continue;

            bucket.validateOrRecoverBy(validator);
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("FileHashTable");
        sb.append("{amountOfBuckets=").append(amountOfBuckets);
        sb.append(", file=").append(file);
        sb.append(", occupiedSlots=").append(occupiedSlots);
        sb.append(", cleaned=").append(cleaned);
        sb.append(", closed=").append(closed);
        sb.append('}');
        return sb.toString();
    }
}
