package com.github.zhongl.ex.nio;

import com.google.common.cache.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ReadOnlyMappedBuffers {

    public static final long DURATION = Long.getLong("ipage.buffer.cache.duration", 1000L);

    private static final Cache<File, MappedByteBuffer> CACHE;

    static {
        CACHE = CacheBuilder.newBuilder()
                            .expireAfterAccess(DURATION, TimeUnit.MILLISECONDS)
                            .removalListener(new RemovalListener<File, MappedByteBuffer>() {
                                @Override
                                public void onRemoval(RemovalNotification<File, MappedByteBuffer> notification) {
                                    DirectByteBufferCleaner.clean(notification.getValue());
                                }
                            })
                            .build(new CacheLoader<File, MappedByteBuffer>() {
                                @Override
                                public MappedByteBuffer load(File key) throws Exception {
                                    return FileChannels.getOrOpen(key).map(READ_ONLY, 0L, key.length());
                                }
                            });
    }


    public static ByteBuffer getOrMap(File file) {
        checkNotNull(file);
        checkState(file.exists());

        MappedByteBuffer buffer = CACHE.getUnchecked(file);
        if (ByteBuffers.lengthOf(buffer) == file.length()) return buffer;

        clearMappedOf(file);
        return CACHE.getUnchecked(file);
    }

    public static void clearMappedOf(File file) {
        CACHE.invalidate(file);
        CACHE.cleanUp();
    }

    public static void clearAll() {
        CACHE.invalidateAll();
        CACHE.cleanUp();
    }
}
