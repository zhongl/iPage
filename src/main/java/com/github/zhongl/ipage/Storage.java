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
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@MBean
public class Storage<V> implements Iterable<V> {

    protected volatile Snapshot<V> snapshot;

    protected final File dir;
    protected final File head;
    protected final File pages;
    protected final Codec<V> codec;

    private volatile long lastMergeElapseMillis;

    protected Storage(File dir, Codec<V> codec) throws IOException {
        this.dir = dir;
        this.codec = codec;
        this.head = new File(dir, "HEAD");
        this.pages = tryMkdir(new File(dir, "pages"));

        this.snapshot = loadHead();
    }

    @ManagedAttribute
    public int getSize() {
        return snapshot.size();
    }

    @ManagedAttribute
    public long getLastMergeElapseMillis() {
        return lastMergeElapseMillis;
    }

    @ManagedAttribute
    public int getAlives() {
        return snapshot.alives();
    }

    public void merge(final Collection<Entry<Key, V>> appendings, Collection<Key> removings, FutureCallback<Void> flushedCallback) {
        // TODO log merge starting

        try {
            Stopwatch stopwatch = new Stopwatch().start();
            String snapshotName = snapshot.merge(appendings, removings);
            lastMergeElapseMillis = stopwatch.elapsedMillis();

            setHead(snapshotName);
            snapshot = new Snapshot<V>(pages, snapshotName, codec);
            flushedCallback.onSuccess(Nils.VOID);
            // TODO log merge ending
        } catch (Throwable t) {
            flushedCallback.onFailure(t);
            // TODO log merge error
            return;
        } finally {
            cleanUp();
        }
    }

    public V get(Key key) {
        return snapshot.get(key);
    }

    @Override
    public Iterator<V> iterator() {
        return snapshot.iterator();
    }

    private File tryMkdir(File dir) {
        if (!dir.exists()) checkState(dir.mkdirs());
        return dir;
    }

    protected void setHead(String snapshotName) throws IOException {
        Files.write(snapshotName + "\n", head, Charset.defaultCharset());
    }

    protected Snapshot<V> loadHead() throws IOException {
        if (!head.exists()) return new Snapshot<V>(pages, "null.s", codec);
        String name = Files.readFirstLine(head, Charset.defaultCharset());
        return new Snapshot<V>(pages, name, codec);
    }

    protected void cleanUp() {
        File[] files = pages.listFiles();
        for (File file : files) {
            if (!snapshot.isLinkTo(file)) checkState(file.delete());
        }
    }


}
