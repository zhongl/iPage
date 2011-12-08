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

import com.github.zhongl.util.FileNumberNameComparator;
import com.github.zhongl.util.NumberFileNameFilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public final class IndexBuilder {

    private static final int UNSET = -1;
    private final File baseDir;
    private int initialBucketSize = UNSET;

    IndexBuilder(File dir) {
        if (!dir.exists()) checkState(dir.mkdirs(), "Can not create directory: %s", dir);
        checkArgument(dir.isDirectory(), "%s should be a directory.", dir);
        baseDir = dir;
    }

    public IndexBuilder initialBucketSize(int value) {
        checkState(initialBucketSize == UNSET, "Initial bucket amountOfBuckets can only set once.");
        initialBucketSize = value;
        return this;
    }

    public Index build() throws IOException {
        List<FileHashTable> fileHashTables = loadExistFileHashTables();
        initialBucketSize = (initialBucketSize == UNSET) ? FileHashTable.DEFAULT_SIZE : initialBucketSize;
        return new Index(baseDir, initialBucketSize, fileHashTables);
    }

    private List<FileHashTable> loadExistFileHashTables() throws IOException {
        File[] files = baseDir.listFiles(new NumberFileNameFilter());
        Arrays.sort(files, new FileNumberNameComparator());
        ArrayList<FileHashTable> fileHashTableList = new ArrayList<FileHashTable>(files.length);
        for (File file : files) {
            checkState((file.length() % Bucket.LENGTH == 0), "FileHashTable %s has invalid length %s", file, file.length());
            int buckets = (int) (file.length() / Bucket.LENGTH);
            fileHashTableList.add(0, new FileHashTable(file, buckets));
        }
        return fileHashTableList;
    }
}
