package com.github.zhongl.ex.util;

import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class SnapshotKeeper<S extends Snapshot> implements Closable {
    protected volatile S currentSnapshot;

    protected SnapshotKeeper(File dir, final Factory<S> factory) throws IOException {
        List<S> list = new FilesLoader<S>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<S>() {
                    @Override
                    public S transform(File file, boolean last) throws IOException {
                        return factory.create(file);
                    }
                }
        ).loadTo(new ArrayList<S>());

        if (list.isEmpty()) {
            currentSnapshot = factory.create(new File(dir, "0"));
        } else {
            // In this case, it means keeper merged failed last time because of crash.
            // So the simplest way is keep only the first binder, and remove rest, then wait for recovery.
            currentSnapshot = list.remove(0);
            for (S s : list) s.remove();
        }
    }

    @Override
    public void close() {
        currentSnapshot.close();
    }

    protected interface Factory<T> {
        T create(File file) throws IOException;
    }
}

