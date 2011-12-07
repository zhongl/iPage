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
import com.github.zhongl.ipage.OverflowException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * {@link Index}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class Index implements Closeable, ValidateOrRecover<Slot, IOException> {

    private final File baseDir;
    private final int initialBucketSize;
    private final List<FileHashTable> fileHashTables;

    Index(File baseDir, int initialBucketSize, List<FileHashTable> fileHashTables) {
        this.baseDir = baseDir;
        this.initialBucketSize = initialBucketSize;
        this.fileHashTables = fileHashTables;
    }

    public Long put(Md5Key key, Long offset) throws IOException {
        try {
            return lastRecentlyUsedBuckets().put(key, offset);
        } catch (OverflowException e) {
            grow();
            return put(key, offset);
        }
    }

    private FileHashTable grow() throws IOException {
        int no = fileHashTables.isEmpty() ? 0 : serialNumber(lastRecentlyUsedBuckets()) + 1;
        int size = fileHashTables.isEmpty() ? initialBucketSize : lastRecentlyUsedBuckets().buckets() * 2;
        FileHashTable fileHashTable = new FileHashTable(new File(baseDir, no + ""), size);
        fileHashTables.add(0, fileHashTable);
        return fileHashTable;
    }

    private int serialNumber(FileHashTable fileHashTable) {
        return Integer.parseInt(fileHashTable.file().getName());
    }

    private FileHashTable lastRecentlyUsedBuckets() throws IOException {
        if (fileHashTables.isEmpty()) return grow();
        return fileHashTables.get(0);
    }

    public Long get(Md5Key key) throws IOException {
        if (fileHashTables.isEmpty()) return FileHashTable.NULL_OFFSET;
        Long offset = lastRecentlyUsedBuckets().get(key);
        if (offset != null) return offset;
        return tryRemoveFromOthersAndMigrate(key, true);
    }

    public Long remove(Md5Key key) throws IOException {
        if (fileHashTables.isEmpty()) return FileHashTable.NULL_OFFSET;
        Long offset = lastRecentlyUsedBuckets().remove(key);
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

    private Long tryRemoveFromOthersAndMigrate(Md5Key key, boolean migrate) throws IOException {
        for (int i = 1; i < fileHashTables.size(); i++) {
            FileHashTable fileHashTable = fileHashTables.get(i);
            Long offset = fileHashTable.remove(key);
            if (offset != null) {
                if (migrate)
                    lastRecentlyUsedBuckets().put(key, offset); // migrate index to last recently used fileHashTable
                if (fileHashTable.isEmpty()) fileHashTable.clean();
                return offset;
            }
        }
        return FileHashTable.NULL_OFFSET;
    }

    @Override
    public boolean validateOrRecoverBy(Validator<Slot, IOException> validator) throws IOException {
        for (FileHashTable fileHashTable : fileHashTables) {
            if (fileHashTable.validateOrRecoverBy(validator)) continue;
            return false;
        }
        return true;
    }

    public static IndexBuilder baseOn(File dir) {
        return new IndexBuilder(dir);
    }

}

