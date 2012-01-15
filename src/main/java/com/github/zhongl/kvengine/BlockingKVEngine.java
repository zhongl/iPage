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

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class BlockingKVEngine {

    private static final String ERROR_MESSAGE = "Too many tasks to submit.";
    private final KVEngine delegate;

    public BlockingKVEngine(KVEngine delegate) {this.delegate = delegate;}

    public void shutdown() throws InterruptedException {delegate.shutdown();}

    public void startup() {delegate.startup();}

    public byte[] put(Md5Key key, byte[] value) throws IOException, InterruptedException {
        Sync<byte[]> callback = new Sync<byte[]>();
        checkState(delegate.put(key, value, callback), ERROR_MESSAGE);
        return callback.get();
    }

    public byte[] get(Md5Key key) throws IOException, InterruptedException {
        Sync<byte[]> callback = new Sync<byte[]>();
        checkState(delegate.get(key, callback), ERROR_MESSAGE);
        return callback.get();
    }

    public byte[] remove(Md5Key key) throws IOException, InterruptedException {
        Sync<byte[]> callback = new Sync<byte[]>();
        checkState(delegate.remove(key, callback), ERROR_MESSAGE);
        return callback.get();
    }

    public Iterator<byte[]> valueIterator() { return delegate.iterator(); }


}
