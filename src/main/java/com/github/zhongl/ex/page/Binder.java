package com.github.zhongl.ex.page;

import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Binder implements Closable {

    private final File dir;
    private final LinkedList<Page> pages;

    public Binder(File dir) throws IOException {
        this.dir = dir;
        pages = loadOrInitialize();
    }

    public <T> Cursor<T> append(T value, boolean force) throws IOException {
        return pages.getLast().append(value, force, new OverflowCallback<T>() {
            @Override
            public Cursor<T> onOverflow(T value, boolean force) throws IOException {
                Page page = newPage(pages.getLast());
                Cursor<T> cursor = page.append(value, force, THROW_BY_OVERFLOW);
                pages.addLast(page);
                return cursor;
            }
        });
    }

    @Override
    public void close() {
        for (Page page : pages) page.close();
    }

    private LinkedList<Page> loadOrInitialize() throws IOException {
        LinkedList<Page> list = new FilesLoader<Page>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<Page>() {
                    @Override
                    public Page transform(File file, boolean last) throws IOException {
                        return newPage(file, Long.parseLong(file.getName()));
                    }
                }).loadTo(new LinkedList<Page>());

        if (list.isEmpty()) list.add(newPage(null));
        return list;
    }

    private Page newPage(Page last) {
        long number = newPageNumber(last);
        return newPage(new File(dir, number + ""), number);
    }

    public void foreach(Function function) {
        // TODO foreach
    }

    public void foreachBetween(Cursor<?> from, Cursor<?> to, Function function) {
        // TODO foreach
    }

    public void applyPageContains(Cursor<?> cursor, Function function) {
        // TODO
    }

    protected abstract Page newPage(File file, long number);

    protected abstract long newPageNumber(@Nullable Page last);

    /** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
    public static interface Function {
        void apply(Page page);
    }
}
