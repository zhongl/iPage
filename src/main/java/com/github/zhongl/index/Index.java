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

import com.github.zhongl.builder.*;
import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;
import com.github.zhongl.util.FileHandler;
import com.github.zhongl.util.NumberNamedFilesLoader;

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
public class Index implements Closeable, ValidateOrRecover<Slot, IOException> {

    private final File baseDir;
    private final int initialBucketSize;
    private final List<FileHashTable> fileHashTables;

    Index(File baseDir, int initialBucketSize) throws IOException {
        this.baseDir = baseDir;
        this.initialBucketSize = initialBucketSize;
        this.fileHashTables = loadExistFileHashTables();
    }

    public Long put(Md5Key key, Long offset) throws IOException {
        try {
            return lastRecentlyUsedBuckets().put(key, offset);
        } catch (IndexOutOfBoundsException e) { // empty
        } catch (BufferOverflowException e) { } // chunk no space for appending
        grow();
        return put(key, offset);
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

    @Override
    public boolean validateOrRecoverBy(Validator<Slot, IOException> validator) throws IOException {
        for (FileHashTable fileHashTable : fileHashTables) {
            if (fileHashTable.validateOrRecoverBy(validator)) continue;
            return false;
        }
        return true;
    }

    private List<FileHashTable> loadExistFileHashTables() throws IOException {
        baseDir.mkdirs();
        checkArgument(baseDir.isDirectory(), "%s should be a directory.", baseDir);
        return new NumberNamedFilesLoader<FileHashTable>(baseDir, new FileHandler<FileHashTable>() {
            @Override
            public FileHashTable handle(File file, boolean last) throws IOException {
                checkState((file.length() % Bucket.LENGTH == 0), "FileHashTable %s has invalid length %s", file, file.length());
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

    private Long tryRemoveFromOthersAndMigrate(Md5Key key, boolean migrate) throws IOException {
        for (int i = 0; i < fileHashTables.size() - 1; i++) {
            FileHashTable fileHashTable = fileHashTables.get(i);
            Long offset = fileHashTable.remove(key);
            if (offset != null) {
                if (migrate)
                    lastRecentlyUsedBuckets().put(key, offset); // migrate index to last recently used fileHashTable
                if (fileHashTable.isEmpty()) {
                    fileHashTable.clean();
                    fileHashTables.remove(i);
                }
                return offset;
            }
        }
        return FileHashTable.NULL_OFFSET;
    }

    public static Builder baseOn(File dir) {
        Builder builder = Builders.newInstanceOf(Builder.class);
        builder.dir(dir);
        return builder;
    }

    public static interface Builder extends BuilderConvention {

        @OptionIndex(0)
        @NotNull
        Builder dir(File value);

        @OptionIndex(1)
        @GreaterThan("0")
        @DefaultValue("256")
        Builder initialBucketSize(int value);

        Index build();
    }
}

