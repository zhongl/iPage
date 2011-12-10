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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class Sync<T> implements FutureCallback<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private T result;
    private Throwable t;

    @Override
    public void onSuccess(T result) {
        this.result = result;
        latch.countDown();
    }

    public T get() throws IOException, InterruptedException {
        latch.await();
        Throwables.propagateIfPossible(t, IOException.class); // cast IOException and throw
        if (t != null) Throwables.propagate(t); // cast RuntimeException Or Error and throw
        return result;
    }

    @Override
    public void onFailure(Throwable t) {
        this.t = t;
        latch.countDown();
    }
}
