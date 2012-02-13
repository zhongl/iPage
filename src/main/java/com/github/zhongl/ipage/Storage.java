/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.io.Files;
import com.google.common.util.concurrent.FutureCallback;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Storage<V> implements Iterable<V> {

    protected volatile Snapshot<V> snapshot;

    protected final File dir;
    protected final File head;
    protected final File pages;
    protected final File tmp;
    protected final Codec<V> codec;

    protected Storage(File dir, Codec<V> codec) throws IOException {
        this.dir = dir;
        this.codec = codec;
        this.head = new File(dir, "HEAD");

        this.pages = new File(dir, "pages");
        tryMkdir(pages);

        this.tmp = new File(dir, "tmp");
        tryMkdir(tmp);

        this.snapshot = loadHead();
        cleanUp();
    }

    public void merge(final Collection<Entry<Key, V>> appendings, Collection<Key> removings, FutureCallback<Void> flushedCallback) {
        if (appendings.isEmpty() && removings.isEmpty()) return;

        try {
            commit(snapshot.merge(appendings, removings, tmp));
        } catch (Throwable t) {
            rollback();
            flushedCallback.onFailure(t);
            return;
        }
        flushedCallback.onSuccess(Nils.VOID);
    }

    public V get(Key key) {
        return snapshot.get(key);
    }

    @Override
    public Iterator<V> iterator() {
        return snapshot.iterator();
    }

    protected void commit(String snapshotName) throws IOException {
        for (File file : tmp.listFiles()) {
            checkState(file.renameTo(new File(pages, file.getName())));
        }
        Snapshot previous = snapshot;
        setHead(snapshotName);
        snapshot = snapshot(new File(pages, snapshotName));
        previous.delete();
    }

    private void tryMkdir(File dir) {
        if (!dir.exists()) checkState(dir.mkdirs());
    }

    protected Snapshot<V> snapshot(File file) throws IOException {
        return new Snapshot<V>(file, codec);
    }

    protected void rollback() {
        if (tmp.exists()) {
            for (File file : tmp.listFiles()) checkState(file.delete());
        }
    }

    protected void setHead(String snapshotName) throws IOException {
        Files.write(snapshotName + "\n", head, Charset.defaultCharset());
    }

    protected Snapshot<V> loadHead() throws IOException {
        if (head.exists()) {
            String name = Files.readFirstLine(head, Charset.defaultCharset());
            return snapshot(new File(pages, name));
        }
        return snapshot(null);
    }

    protected void cleanUp() {
        // TODO cleanUp
    }


}
