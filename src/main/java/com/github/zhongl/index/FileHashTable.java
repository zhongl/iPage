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

package com.github.zhongl.index;

import com.github.zhongl.sequence.Cursor;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

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
public final class FileHashTable implements ValidateOrRecover {

    public static final int DEFAULT_SIZE = 256;

    private final int amountOfBuckets;
    private final File file;

    private volatile int occupiedSlots;
    private final FileChannel channel;

    public FileHashTable(File file, int buckets) throws IOException {
        this.file = file;
        amountOfBuckets = buckets > 0 ? buckets : DEFAULT_SIZE;
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(amountOfBuckets * Bucket.LENGTH);
        channel = randomAccessFile.getChannel();
        calculateOccupiedSlots();
    }

    public int amountOfBuckets() {
        return amountOfBuckets;
    }

    public int size() {
        return occupiedSlots;
    }

    public Cursor put(Md5Key key, Cursor cursor) throws IOException {
        Bucket bucket = buckets(hashAndMod(key));
        Cursor preCursor = bucket.put(key, cursor);
        bucket.updateCRC();
        if (preCursor == null) occupiedSlots++; // add a new key
        return preCursor;
    }

    public Cursor get(Md5Key key) throws IOException {
        return buckets(hashAndMod(key)).get(key);
    }

    public Cursor remove(Md5Key key) throws IOException {
        Bucket bucket = buckets(hashAndMod(key));
        Cursor cursor = bucket.remove(key);
        if (cursor != null) { // remove an exist key
            bucket.updateCRC();
            occupiedSlots--;
        }
        return cursor;
    }

    public void close() throws IOException {
        channel.close();
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
        return new Bucket(i * Bucket.LENGTH, channel);
    }

    public void flush() throws IOException {
        channel.force(true);
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
    public boolean validateOrRecoverBy(Validator validator) throws IOException {
        for (int i = 0; i < amountOfBuckets; i++) {
            Bucket bucket = buckets(i);
            if (bucket.checkCRC()) continue;

            bucket.validateOrRecoverBy(validator);
            return false;
        }
        return true;
    }

}
