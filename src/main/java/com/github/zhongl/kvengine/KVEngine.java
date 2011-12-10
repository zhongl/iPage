/*
 * Copyright 2011 zhongl
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
import com.github.zhongl.ipage.Cursor;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class KVEngine<T> extends Engine {

    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final DataIntegerity dataIntegerity;
    private final Operation<T> operation;

    public KVEngine(long pollTimeout, int backlog, DataIntegerity dataIntegerity, Operation<T> operation) {
        super(pollTimeout, DEFAULT_TIME_UNIT, backlog);
        this.dataIntegerity = dataIntegerity;
        this.operation = operation;
    }

    @Override
    public void shutdown() throws InterruptedException {
        super.shutdown();
        try {
            operation.close();
            dataIntegerity.safeClose();
        } catch (IOException e) {
            e.printStackTrace();  // TODO log e
        }
    }

    public static <V> KVEngineBuilder<V> baseOn(File dir) {
        return new KVEngineBuilder<V>(dir);
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean put(Md5Key key, T value, FutureCallback<T> callback) {
        return submit(operation.put(key, value, callback));
    }

    public boolean get(Md5Key key, FutureCallback<T> callback) {
        return submit(operation.get(key, callback));
    }

    public boolean remove(Md5Key key, FutureCallback<T> callback) {
        return submit(operation.remove(key, callback));
    }

    public boolean next(Cursor<Entry<T>> entryCursor, FutureCallback<Cursor<Entry<T>>> callback) {
        return submit(operation.next(entryCursor, callback));
    }

    public boolean garbageCollect(long survivorOffset, FutureCallback<Long> callback) {
        return submit(operation.garbageCollect(survivorOffset, callback));
    }

    @Override
    protected void hearbeat() {
        operation.tryGroupCommitByElapse();
    }

}
