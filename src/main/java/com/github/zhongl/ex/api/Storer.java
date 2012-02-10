package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.blocks.Blocks;
import com.github.zhongl.ex.blocks.FileBitmap;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Checkpoint;
import com.github.zhongl.ex.journal.CheckpointKeeper;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.github.zhongl.ex.util.Nils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.github.zhongl.ex.actor.Actors.actor;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Storer extends Actor implements Durable {
    private Blocks blocks;
    private FileBitmap fileBitmap;
    private CheckpointKeeper checkpointKeeper;

    public Storer(File dir) {
        super();
//        blocks = new Blocks(new File);
    }

    @Override
    public void merge(final List<Entry<Md5Key, Cursor>> removings, final List<Entry<Md5Key, byte[]>> appendings, final Checkpoint checkpoint) {
        if (checkpointKeeper.last().compareTo(checkpoint) > 0) return;

        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                List<Future<Cursor>> futures = new ArrayList<Future<Cursor>>();
                for (Entry<Md5Key, byte[]> entry : appendings) {
                    CallbackFuture<Cursor> callback = new CallbackFuture<Cursor>();
                    blocks.append(entry, callback);
                    futures.add(callback);
                }
                blocks.force();

                List<Cursor> appendingCursors = transform(futures, new Function<Future<Cursor>, Cursor>() {
                    @Override
                    public Cursor apply(@Nullable Future<Cursor> input) {
                        return FutureCallbacks.getUnchecked(input);
                    }
                });

                List<Cursor> removingCursors = transform(removings, new Function<Entry<Md5Key, Cursor>, Cursor>() {
                    @Override
                    public Cursor apply(@Nullable Entry<Md5Key, Cursor> input) {
                        return input.value();
                    }
                });

                fileBitmap.merge(removingCursors, appendingCursors);

                checkpointKeeper.last(checkpoint);

                actor(Mergable.class).merge(transform(removings, new Function<Entry<Md5Key, Cursor>, Entry<Md5Key, Cursor>>() {
                    @Override
                    public Entry<Md5Key, Cursor> apply(@Nullable Entry<Md5Key, Cursor> input) {
                        return new Entry<Md5Key, Cursor>(input.key(), Nils.CURSOR);
                    }
                }), checkpoint);

                return Nils.VOID;
            }
        });

    }

    @Override
    public void get(final Cursor cursor, final FutureCallback<byte[]> callback) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    callback.onSuccess((byte[]) blocks.get(cursor));
                } catch (IOException e) {
                    callback.onFailure(e);
                }
                return Nils.VOID;
            }
        });
    }

    static <I, O> List<O> transform(final List<I> src, final Function<I, O> function) {
        return new AbstractList<O>() {
            @Override
            public O get(int index) {
                return function.apply(src.get(index));
            }

            @Override
            public int size() {
                return src.size();
            }
        };
    }
}
