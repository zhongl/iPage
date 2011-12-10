/*
 * Copyright 2011 zhongl
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

package com.github.zhongl.kvengine;

import com.github.zhongl.index.Md5Key;
import com.github.zhongl.ipage.Cursor;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class BlockingKVEngine<T> {

    private final KVEngine<T> delegate;

    public BlockingKVEngine(KVEngine<T> delegate) {this.delegate = delegate;}

    public void shutdown() throws InterruptedException {delegate.shutdown();}

    public void startup() {delegate.startup();}

    public T put(Md5Key key, T value) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        checkState(delegate.put(key, value, callback), "Too many tasks to submit.");
        return callback.get();
    }

    public T get(Md5Key key) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        checkState(delegate.get(key, callback), "Too many tasks to submit.");
        return callback.get();
    }

    public T remove(Md5Key key) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        checkState(delegate.remove(key, callback), "Too many tasks to submit.");
        return callback.get();
    }

    public Iterator<T> valueIterator() {
        return new ValueIterator<T>(delegate);
    }

    public long garbageCollect() throws IOException, InterruptedException {
        long collectedLength = 0L;
        long survivorOffset;
        Cursor<Entry<T>> cursor = Cursor.begin(-1L);
        while (true) {
            survivorOffset = cursor.offset();
            Sync<Cursor<Entry<T>>> cursorCallback = new Sync<Cursor<Entry<T>>>();
            checkState(delegate.next(cursor, cursorCallback), "Too many tasks to submit.");
            cursor = cursorCallback.get();
            if (cursor.isEnd()) break;
            if (cursor.lastValue() == null) continue;
            Sync<Long> longCallback = new Sync<Long>();
            checkState(delegate.garbageCollect(survivorOffset, longCallback), "Too many tasks to submit.");
            collectedLength += longCallback.get();
        }

        return collectedLength;
    }

}
