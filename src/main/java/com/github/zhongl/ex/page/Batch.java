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

package com.github.zhongl.ex.page;

import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public abstract class Batch {

    private boolean notWrote = true;

    public Cursor append(final Object object) {
        checkNotNull(object);
        checkState(notWrote);
        return _append(object);
    }

    int writeAndForceTo(FileChannel channel) {
        notWrote = false;
        return _writeAndForceTo(channel);
    }

    protected abstract Cursor _append(Object object);

    protected abstract int _writeAndForceTo(FileChannel channel);

    public void append(Object value, FutureCallback<Cursor> forceCallback) {
        checkState(notWrote);
        _append(checkNotNull(value), checkNotNull(forceCallback));
    }

    protected abstract void _append(Object value, FutureCallback<Cursor> callback);
}
