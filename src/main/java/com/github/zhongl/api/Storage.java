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

package com.github.zhongl.api;

import com.github.zhongl.index.Difference;
import com.github.zhongl.index.Index;
import com.github.zhongl.index.Key;
import com.github.zhongl.page.Element;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.collect.Collections2.transform;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@MBean
class Storage<V> implements Iterable<V> {
    private final Logger logger;
    private final Snapshot<V> snapshot;
    private final DefragPolicy defragPolicy;
    private final IndexFactory indexFactory;

    private volatile Behavior lastBehavior = Behavior.NONE;
    private volatile long lastBehaviorElapseMillis;

    Storage(Snapshot<V> snapshot, IndexFactory indexFactory, DefragPolicy defragPolicy) {
        this.logger = Logger.getLogger(getClass().getName());
        this.snapshot = snapshot;
        this.defragPolicy = defragPolicy;
        this.indexFactory = indexFactory;
    }

    public void merge(
            Collection<WriteOperation<Entry<Key, V>>> addOrUpdates,
            Collection<WriteOperation<Key>> removes,
            FutureCallback<Void> flushedCallback
    ) {
        Stopwatch stopwatch = new Stopwatch().start();
        try {
            if (defragPolicy.evaluate(snapshot.aliveSize(), addOrUpdates.size() - removes.size())) {
                defrag(addOrUpdates, removes);
                lastBehavior = Behavior.DEFRAG;
            } else {
                append(addOrUpdates, removes);
                lastBehavior = Behavior.APPEND;
            }
            snapshot.update();
            onSuccess(addOrUpdates);
            onSuccess(removes);
        } catch (OutOfMemoryError e) {
            logger.log(Level.WARNING, "Reject add or update operations because ", e);
            onFailure(addOrUpdates, e);
            if (!addOrUpdates.isEmpty() // avoid recursion merging over 2 level
                    && !removes.isEmpty())
                merge(Collections.<WriteOperation<Entry<Key, V>>>emptySet(), removes, flushedCallback);
        } catch (Throwable e) {
            logger.log(Level.WARNING, "Merge failed because ", e);
            onFailure(addOrUpdates, e);
            onFailure(removes, e);
            lastBehavior = Behavior.FAIL;
        } finally {
            flushedCallback.onSuccess(Nils.VOID);
            snapshot.cleanUp();
            lastBehaviorElapseMillis = stopwatch.stop().elapsedMillis();
        }
    }

    public V get(Key key) { return snapshot.get(key); }

    @Override
    public Iterator<V> iterator() { return snapshot.iterator(); }

    @ManagedAttribute
    public String getLastBehavior() { return lastBehavior.name(); }

    @ManagedAttribute
    public long getLastBehaviorElapseMillis() { return lastBehaviorElapseMillis; }

    @ManagedAttribute
    public long getDiskOccupiedBytes() { return snapshot.diskOccupiedBytes(); }

    private void append(Collection<WriteOperation<Entry<Key, V>>> addOrUpdates,
                        Collection<WriteOperation<Key>> removes) throws IOException {
        append(addOrUpdates, removes, new Difference(new TreeSet<Index>()));
    }

    private void defrag(Collection<WriteOperation<Entry<Key, V>>> addOrUpdates,
                        Collection<WriteOperation<Key>> removes) throws IOException {
        final Difference difference = new Difference(new TreeSet<Index>());
        final Collection<Key> removedKeys = Collections2.transform(removes, new Function<WriteOperation<Key>, Key>() {
            @Override
            public Key apply(WriteOperation<Key> operation) {
                return operation.attachement();
            }
        });

        snapshot.defrag(
                new Predicate<Element<Entry<Key, V>>>() {
                    @Override
                    public boolean apply(final Element<Entry<Key, V>> element) {
                        return !removedKeys.contains(element.value().key()) && !snapshot.isRemoved(element);
                    }
                },
                new Function<Element<Entry<Key, V>>, Void>() {
                    @Override
                    public Void apply(Element<Entry<Key, V>> element) {
                        difference.add(indexFactory.index(element.value().key(), element.range()));
                        return null;
                    }
                }
        );

        append(addOrUpdates, removes, difference);
    }

    private void append(Collection<WriteOperation<Entry<Key, V>>> addOrUpdates,
                        Collection<WriteOperation<Key>> removes,
                        final Difference difference) throws IOException {
        if (!addOrUpdates.isEmpty()) {
            snapshot.append(
                    transform(
                            addOrUpdates,
                            new Function<WriteOperation<Entry<Key, V>>, Entry<Key, V>>() {
                                @Override
                                public Entry<Key, V> apply(WriteOperation<Entry<Key, V>> operation) {
                                    return operation.attachement();
                                }
                            }
                    ),
                    new Function<Element<Entry<Key, V>>, Void>() {
                        @Override
                        public Void apply(Element<Entry<Key, V>> element) {
                            difference.add(indexFactory.index(element.value().key(), element.range()));
                            return null;
                        }
                    });
        }

        if (!removes.isEmpty()) {
            difference.addAll(transform(removes, new Function<WriteOperation<Key>, Index>() {
                @Override
                public Index apply(WriteOperation<Key> operation) {
                    return indexFactory.removedIndex(operation.attachement());
                }
            }));
        }

        snapshot.merge(difference);
    }

    private static void onSuccess(Collection<? extends FutureCallback<Void>> callbacks) {
        for (FutureCallback<Void> callback : callbacks) callback.onSuccess(null);
    }

    private static void onFailure(Collection<? extends FutureCallback<Void>> callbacks, Throwable t) {
        for (FutureCallback<Void> callback : callbacks) callback.onFailure(t);
    }

    private enum Behavior {NONE, DEFRAG, APPEND, FAIL}
}
