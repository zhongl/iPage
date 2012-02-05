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

package com.github.zhongl.ex.actor;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class CallbackFuture<T> extends AbstractFuture<T> implements FutureCallback<T> {

    @Override
    public void onSuccess(T result) {
        set(result);
    }

    @Override
    public void onFailure(Throwable t) {
        setException(t);
    }
}
