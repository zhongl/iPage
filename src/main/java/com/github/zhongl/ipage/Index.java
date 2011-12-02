package com.github.zhongl.ipage;

import com.github.zhongl.util.FileNumberNameComparator;
import com.github.zhongl.util.NumberFileNameFilter;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * {@link Index}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class Index implements Closeable {

    private final File baseDir;
    private final int initBucketSize;
    private final List<Buckets> bucketsList;

    private Index(File baseDir, int initBucketSize, List<Buckets> bucketsList) {
        this.baseDir = baseDir;
        this.initBucketSize = initBucketSize;
        this.bucketsList = bucketsList;
    }

    public Long put(Md5Key key, Long offset) throws IOException {
        try {
            return lastRecentlyUsedBuckets().put(key, offset);
        } catch (OverflowException e) {
            grow();
            return put(key, offset);
        }
    }

    private Buckets grow() throws IOException {
        int no = bucketsList.isEmpty() ? 0 : lastRecentlyUsedBuckets().no() + 1;
        int size = bucketsList.isEmpty() ? initBucketSize : lastRecentlyUsedBuckets().size() * 2;
        Buckets buckets = new Buckets(new File(baseDir, no + ""), size);
        bucketsList.add(0, buckets);
        return buckets;
    }

    private Buckets lastRecentlyUsedBuckets() throws IOException {
        if (bucketsList.isEmpty()) return grow();
        return bucketsList.get(0);
    }

    public Long get(Md5Key key) throws IOException {
        if (bucketsList.isEmpty()) return Buckets.NULL_OFFSET;
        Long offset = lastRecentlyUsedBuckets().get(key);
        if (offset != null) return offset;
        return tryRemoveFromOthersAndMigrate(key, true);
    }

    public Long remove(Md5Key key) throws IOException {
        if (bucketsList.isEmpty()) return Buckets.NULL_OFFSET;
        Long offset = lastRecentlyUsedBuckets().remove(key);
        if (offset != null) return offset;
        return tryRemoveFromOthersAndMigrate(key, false);
    }

    @Override
    public void close() throws IOException {
        for (Buckets buckets : bucketsList) {
            buckets.close();
        }
    }

    public void flush() throws IOException {
        for (Buckets buckets : bucketsList) {
            buckets.flush();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Index");
        sb.append("{baseDir=").append(baseDir);
        sb.append(", initialBucketSize=").append(initBucketSize);
        sb.append(", bucketsList=").append(bucketsList);
        sb.append('}');
        return sb.toString();
    }

    private Long tryRemoveFromOthersAndMigrate(Md5Key key, boolean migrate) throws IOException {
        for (int i = 1; i < bucketsList.size(); i++) {
            Buckets buckets = bucketsList.get(i);
            Long offset = buckets.remove(key);
            if (offset != null) {
                if (migrate) lastRecentlyUsedBuckets().put(key, offset); // migrate index to last recently used buckets
                buckets.cleanupIfAllKeyRemoved();
                return offset;
            }
        }
        return Buckets.NULL_OFFSET;
    }


    public static Builder baseOn(File dir) {
        return new Builder(dir);
    }

    public static final class Builder {

        private static final int UNSET = -1;
        private final File baseDir;
        private int initBucketSize = UNSET;

        public Builder(File dir) {
            if (!dir.exists()) checkState(dir.mkdirs(), "Can not create directory: %s", dir);
            checkArgument(dir.isDirectory(), "%s should be a directory.", dir);
            baseDir = dir;
        }

        public Builder initBucketSize(int value) {
            checkState(initBucketSize == UNSET, "Initial bucket size can only set once.");
            initBucketSize = value;
            return this;
        }

        public Index build() throws IOException {
            initBucketSize = (initBucketSize == UNSET) ? Buckets.DEFAULT_SIZE : initBucketSize;
            List<Buckets> bucketsList = loadExistBucketsList();
            return new Index(baseDir, initBucketSize, bucketsList);
        }

        private List<Buckets> loadExistBucketsList() throws IOException {
            File[] files = baseDir.listFiles(new NumberFileNameFilter());
            Arrays.sort(files, new FileNumberNameComparator());
            ArrayList<Buckets> bucketsList = new ArrayList<Buckets>(files.length);
            for (File file : files) {
                bucketsList.add(0, new Buckets(file, initBucketSize));
            }
            return bucketsList;
        }
    }
}

