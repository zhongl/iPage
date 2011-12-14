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

import com.github.zhongl.buffer.MappedBufferFile;
import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

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

    private final int amountOfBuckets;
    private final File file;

    private volatile int occupiedSlots;
    private final MappedBufferFile mappedBufferFile;

    public FileHashTable(File file, int buckets) throws IOException {
        this.file = file;
        amountOfBuckets = buckets > 0 ? buckets : DEFAULT_SIZE;
        mappedBufferFile = MappedBufferFile.writeable(file, amountOfBuckets * Bucket.LENGTH);
        calculateOccupiedSlots();
    }

    public int amountOfBuckets() {
        return amountOfBuckets;
    }

    public int size() {
        return occupiedSlots;
    }

    public Long put(Md5Key key, Long offset) throws IOException {
        Bucket bucket = buckets(hashAndMod(key));
        Long preoffset = bucket.put(key, offset);
        bucket.updateCRC();
        if (preoffset == null) occupiedSlots++; // add a new key
        return preoffset;
    }

    public Long get(Md5Key key) throws IOException {
        return buckets(hashAndMod(key)).get(key);
    }

    public Long remove(Md5Key key) throws IOException {
        Bucket bucket = buckets(hashAndMod(key));
        Long preoffset = bucket.remove(key);
        if (preoffset != null) { // remove an exist key
            bucket.updateCRC();
            occupiedSlots--;
        }
        return preoffset;
    }

    public void close() {
        flush();
        mappedBufferFile.release();
    }

    private int hashAndMod(Md5Key key) {
        return Math.abs(key.hashCode()) % amountOfBuckets;
    }

    private void calculateOccupiedSlots() throws IOException {
        for (int i = 0; i < amountOfBuckets; i++) {
            occupiedSlots += buckets(i).occupiedSlots();
        }
    }

    private Bucket buckets(int i) {
        return new Bucket(i * Bucket.LENGTH, mappedBufferFile);
    }

    public void flush() {
        mappedBufferFile.flush();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void clean() {
        checkState(file.delete(), "Can't delete file %s", file);
    }

    public File file() {
        return file;
    }

    @Override
    public boolean validateOrRecoverBy(Validator<Slot, IOException> validator) throws IOException {
        for (int i = 0; i < amountOfBuckets; i++) {
            Bucket bucket = buckets(i);
            if (bucket.checkCRC()) continue;

            bucket.validateOrRecoverBy(validator);
            return false;
        }
        return true;
    }

}
