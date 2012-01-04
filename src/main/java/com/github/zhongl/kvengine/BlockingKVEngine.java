/*
 * Copyright 2012 zhongl
 *
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
import com.github.zhongl.util.Sync;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class BlockingKVEngine<T> {

    private static final String ERROR_MESSAGE = "Too many tasks to submit.";
    private final KVEngine<T> delegate;

    public BlockingKVEngine(KVEngine<T> delegate) {this.delegate = delegate;}

    public void shutdown() throws InterruptedException {delegate.shutdown();}

    public void startup() {delegate.startup();}

    public T put(Md5Key key, T value) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        checkState(delegate.put(key, value, callback), ERROR_MESSAGE);
        return callback.get();
    }

    public T get(Md5Key key) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        checkState(delegate.get(key, callback), ERROR_MESSAGE);
        return callback.get();
    }

    public T remove(Md5Key key) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        checkState(delegate.remove(key, callback), ERROR_MESSAGE);
        return callback.get();
    }

    public Iterator<T> valueIterator() { return new ValueIterator<T>(delegate); }

    @Deprecated
    public long garbageCollect(long begin, long end) throws IOException, InterruptedException {
        Sync<Long> longCallback = new Sync<Long>();
        checkState(delegate.garbageCollect(begin, end, longCallback), ERROR_MESSAGE);
        return longCallback.get();
    }

}
