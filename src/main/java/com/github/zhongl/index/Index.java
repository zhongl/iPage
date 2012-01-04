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
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link Index}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class Index implements Closeable, ValidateOrRecover {

    private final File baseDir;
    private final int initialBucketSize;
    private final List<FileHashTable> fileHashTables;

    public Index(File baseDir, int initialBucketSize) throws IOException {
        this.baseDir = baseDir;
        this.initialBucketSize = initialBucketSize;
        this.fileHashTables = loadExistFileHashTables();
    }

    public Cursor put(Md5Key key, Cursor cursor) throws IOException {
        try {
            if (!fileHashTables.isEmpty()) return lastRecentlyUsedBuckets().put(key, cursor);
        } catch (BufferOverflowException e) { } // chunk no space for appending
        grow();
        return put(key, cursor);
    }


    public Cursor get(Md5Key key) throws IOException {
        if (fileHashTables.isEmpty()) return Cursor.NULL;
        Cursor offset = lastRecentlyUsedBuckets().get(key);
        if (offset != null) return offset;
        return tryRemoveFromOthersAndMigrate(key, true);
    }

    public Cursor remove(Md5Key key) throws IOException {
        if (fileHashTables.isEmpty()) return Cursor.NULL;
        Cursor offset = lastRecentlyUsedBuckets().remove(key);
        if (offset != null) return offset;
        return tryRemoveFromOthersAndMigrate(key, false);
    }

    @Override
    public void close() throws IOException {
        for (FileHashTable fileHashTable : fileHashTables) {
            fileHashTable.close();
        }
    }

    public void flush() throws IOException {
        for (FileHashTable fileHashTable : fileHashTables) {
            fileHashTable.flush();
        }
    }

    @Override
    public boolean validateOrRecoverBy(Validator validator) throws IOException {
        for (FileHashTable fileHashTable : fileHashTables) {
            if (fileHashTable.validateOrRecoverBy(validator)) continue;
            return false;
        }
        return true;
    }

    private List<FileHashTable> loadExistFileHashTables() throws IOException {
        baseDir.mkdirs();
        checkArgument(baseDir.isDirectory(), "%s should be a directory.", baseDir);
        return new FilesLoader<FileHashTable>(baseDir,
                new NumberNamedFilterAndComparator(),
                new Transformer<FileHashTable>() {
                    @Override
                    public FileHashTable transform(File file, boolean last) throws IOException {
                        checkState((file.length() % Bucket.LENGTH == 0), "FileHashTable %s has invalid length %s", file, file
                                .length());
                        int buckets = (int) (file.length() / Bucket.LENGTH);
                        return new FileHashTable(file, buckets);
                    }
                }).loadTo(new ArrayList<FileHashTable>());
    }

    private FileHashTable grow() throws IOException {
        int no = fileHashTables.isEmpty() ? 0 : serialNumber(lastRecentlyUsedBuckets()) + 1;
        int size = fileHashTables.isEmpty() ? initialBucketSize : lastRecentlyUsedBuckets().amountOfBuckets() * 2;
        FileHashTable fileHashTable = new FileHashTable(new File(baseDir, Integer.toString(no)), size);
        fileHashTables.add(fileHashTable);
        return fileHashTable;
    }

    private int serialNumber(FileHashTable fileHashTable) {
        return Integer.parseInt(fileHashTable.file().getName());
    }

    private FileHashTable lastRecentlyUsedBuckets() throws IOException {
        return fileHashTables.get(lastIndex());
    }

    private int lastIndex() {return fileHashTables.size() - 1;}

    private Cursor tryRemoveFromOthersAndMigrate(Md5Key key, boolean migrate) throws IOException {
        for (int i = 0; i < fileHashTables.size() - 1; i++) {
            FileHashTable fileHashTable = fileHashTables.get(i);
            Cursor cursor = fileHashTable.remove(key);
            if (cursor != null) {
                if (migrate)
                    lastRecentlyUsedBuckets().put(key, cursor); // migrate index to last recently used fileHashTable
                if (fileHashTable.isEmpty()) {
                    fileHashTable.clean();
                    fileHashTables.remove(i);
                }
                return cursor;
            }
        }
        return Cursor.NULL;
    }

}

