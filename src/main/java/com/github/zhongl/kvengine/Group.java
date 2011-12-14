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

import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.LinkedList;
import java.util.Queue;

/**
 * {@link Group}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
abstract class Group {

    public static final Group NULL = new Group() {
        @Override
        public void commit() {}

        @Override
        public void rollback(Throwable t) { }

        @Override
        public <T> FutureCallback<T> decorate(FutureCallback<T> callback) {
            return callback;
        }

        @Override
        public void register(FutureCallback<?> callback) { }
    };

    public static Group newInstance() {
        return new DefaultGroup();
    }

    /**
     * Commit all pending groupable {@link com.google.common.util.concurrent.FutureCallback}s, and there
     * {@link com.google.common.util.concurrent.FutureCallback#onSuccess(Object)} will be invoked.
     */
    public abstract void commit();

    /**
     * Rollback all pending groupable {@link com.google.common.util.concurrent.FutureCallback}s, and there
     * {@link com.google.common.util.concurrent.FutureCallback#onFailure(Throwable)} will be invoked.
     *
     * @param t {@link Throwable}
     */
    public abstract void rollback(Throwable t);

    /**
     * Decorate {@link com.google.common.util.concurrent.FutureCallback} to a new one, which can be grouped for
     * committing or rollbacking.
     *
     * @param callback {@link com.google.common.util.concurrent.FutureCallback}
     *
     * @return groupable {@link com.google.common.util.concurrent.FutureCallback}
     */
    public abstract <T> FutureCallback<T> decorate(FutureCallback<T> callback);

    /**
     * Register {@link com.google.common.util.concurrent.FutureCallback} for committing or rollbacking.
     *
     * @param callback {@link com.google.common.util.concurrent.FutureCallback}
     */
    public abstract void register(FutureCallback<?> callback);

    private static class DefaultGroup extends Group {

        private final Queue<GroupableFutureCallback<?>> pendingCallbacks = new LinkedList<GroupableFutureCallback<?>>();

        @Override
        public void commit() {
            while (true) {
                GroupableFutureCallback<?> callback = pendingCallbacks.poll();
                if (callback == null) break;
                callback.commit();
            }
        }

        @Override
        public void rollback(Throwable t) {
            while (true) {
                GroupableFutureCallback<?> callback = pendingCallbacks.poll();
                if (callback == null) break;
                callback.rollback(t);
            }
        }

        @Override
        public <T> FutureCallback<T> decorate(FutureCallback<T> callback) {
            return new GroupableFutureCallback<T>(callback);
        }

        @Override
        public void register(FutureCallback<?> callback) {
            pendingCallbacks.add((GroupableFutureCallback<?>) callback);
        }
    }

    private static class GroupableFutureCallback<T> implements FutureCallback<T> {

        private final FutureCallback<T> delegate;
        private T result;

        private GroupableFutureCallback(FutureCallback<T> delegate) {this.delegate = delegate;}

        @Override
        public void onSuccess(T result) {
            this.result = result;
        }

        @Override
        public void onFailure(Throwable t) {
            delegate.onFailure(t); // trigger failure immediately if it is write exception
        }

        void commit() {
            delegate.onSuccess(result);
        }

        void rollback(Throwable t) {
            delegate.onFailure(t); // trigger rollback failure
        }
    }
}
