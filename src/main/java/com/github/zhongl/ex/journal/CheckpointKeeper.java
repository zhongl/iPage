package com.github.zhongl.ex.journal;

import com.github.zhongl.ex.util.Nils;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.FilterAndComparator;
import com.github.zhongl.util.Transformer;
import com.google.common.base.CharMatcher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CheckpointKeeper {
    public static final String SUFFIX = ".cpt";
    private final File dir;

    private volatile Checkpoint last;

    public CheckpointKeeper(File dir) throws IOException {
        this.dir = dir;
        if (!dir.exists()) checkState(dir.mkdirs());

        List<Checkpoint> checkpoints = new FilesLoader<Checkpoint>(dir, new FilterAndComparator() {
            @Override
            public int compare(File o1, File o2) {
                return checkpoint(o1).compareTo(checkpoint(o2));
            }

            @Override
            public boolean accept(File dir, String name) {
                return name.matches("\\d+\\.cpt");
            }
        }, new Transformer<Checkpoint>() {
            @Override
            public Checkpoint transform(File file, boolean last) throws IOException {
                return last ? checkpoint(file) : null;
            }
        }
        ).loadTo(new ArrayList<Checkpoint>());

        if (checkpoints.isEmpty()) last = Nils.CHECKPOINT;
        else last = checkpoints.get(0);
    }

    private Checkpoint checkpoint(File file) {
        String text = CharMatcher.anyOf(SUFFIX).removeFrom(file.getName());
        return new Checkpoint(text);
    }

    public Checkpoint last() {
        return last;
    }

    public void last(Checkpoint checkpoint) {
        try {
            file(checkpoint).createNewFile();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        file(last).delete();
        last = checkpoint;
    }

    private File file(Checkpoint checkpoint) {
        return new File(dir, checkpoint.value() + SUFFIX);
    }
}
