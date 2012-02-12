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
import com.google.common.base.Function;
import com.google.common.io.Files;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Collections2.transform;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Storage<K, V> implements Iterable<V> {

    protected volatile Snapshot snapshot;

    protected final File dir;
    protected final File head;
    protected final File pages;
    protected final File tmp;

    protected Storage(File dir) throws IOException {
        this.dir = dir;
        this.head = new File(dir, "HEAD");
        this.pages = new File(dir, "pages");
        this.tmp = new File(dir, "tmp");
        this.snapshot = loadHead();
        cleanUp();
    }

    public void merge(final Collection<Entry<K, V>> appendings, Collection<K> removings, FutureCallback<Void> flushedCallback) {
        if (appendings.isEmpty() && removings.isEmpty()) return;
        try {

            String newSnapshotName = snapshot.merge(
                    transform(appendings, new Function<Entry<K, V>, Entry<Key, V>>() {
                        @Override
                        public Entry<Key, V> apply(@Nullable Entry<K, V> entry) {
                            return new Entry<Key, V>(toKey(entry.key()), entry.value());
                        }
                    }),
                    transform(removings, new Function<K, Key>() {
                        @Override
                        public Key apply(@Nullable K key) {
                            return toKey(key);
                        }
                    }),
                    tmp);

            commit(newSnapshotName);
        } catch (Throwable t) {
            rollback();
            flushedCallback.onFailure(t);
            return;
        }
        flushedCallback.onSuccess(Nils.VOID);
    }

    public V get(K key) {
        return snapshot.get(toKey(key));
    }

    @Override
    public Iterator<V> iterator() {
        return snapshot.iterator();
    }

    protected void commit(String snapshotName) throws IOException {
        if (!pages.exists()) pages.mkdirs();
        for (File file : tmp.listFiles()) {
            file.renameTo(new File(pages, file.getName()));
        }
        Snapshot previous = snapshot;
        setHead(snapshotName);
        snapshot = snapshot(new File(pages, snapshotName));
        previous.delete();
    }

    protected Snapshot snapshot(File file) throws IOException {
        // scan files under file, move them to pages and create revision
        return new Snapshot(file) {
            @Override
            protected <T> ByteBuffer encode(Entry<Key, T> entry) {
                return null;  // TODO encode
            }

            @Override
            protected <T> T decode(ByteBuffer buffer) {
                return null;  // TODO decode
            }
        };
    }

    protected void rollback() {
        for (File file : tmp.listFiles()) checkState(file.delete());
    }

    protected void setHead(String snapshotName) throws IOException {
        Files.write(snapshotName, head, Charset.defaultCharset());
    }

    protected Snapshot loadHead() throws IOException {
        if (head.exists()) {
            String name = Files.readFirstLine(head, Charset.defaultCharset());
            return snapshot(new File(pages, name));
        }
        return snapshot(null);
    }

    protected abstract void cleanUp(); // clean tmp and files which were not linked by revision.

    protected abstract Key toKey(K key);

}
