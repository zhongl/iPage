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

package com.github.zhongl.engine;

import com.google.common.util.concurrent.FutureCallback;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public abstract class Task<T> implements Runnable {
    protected final FutureCallback<T> callback;

    public Task(FutureCallback<T> callback) {
        this.callback = callback;
    }

    @Override
    public final void run() {
        try {
            T result = execute();
            callback.onSuccess(result);
        } catch (Throwable t) {
            callback.onFailure(t);
        }
    }

    protected abstract T execute() throws Throwable;
}
