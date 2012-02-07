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

package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Checkpoint;
import com.github.zhongl.ex.journal.Journal;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.Entry;

import java.util.concurrent.Callable;

import static com.github.zhongl.ex.actor.Actors.actor;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public enum QuanlityOfService {
    RELIABLE {
        @Override
        public Callable<Void> append(final Journal journal, final Entry<Md5Key, byte[]> entry) {
            return new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    CallbackFuture<Cursor> callbackFuture = new CallbackFuture<Cursor>();
                    journal.append(entry, callbackFuture);
                    tryForce(journal.checkpoint(callbackFuture.get()));
                    return VOID;
                }
            };
        }
    }, BALANCE {
        @Override
        public Callable<Void> append(final Journal journal, final Entry<Md5Key, byte[]> entry) {
            return new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    CallbackFuture<Cursor> callbackFuture = new CallbackFuture<Cursor>();
                    journal.append(entry, callbackFuture);
                    actor(Updatable.class).update(entry);
                    callbackFuture.get();
                    tryForce(journal.checkpoint(callbackFuture.get()));
                    return VOID;
                }
            };
        }
    }, LATENCY {
        @Override
        public Callable<Void> append(final Journal journal, final Entry<Md5Key, byte[]> entry) {
            return new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    actor(Updatable.class).update(entry);
                    CallbackFuture<Cursor> callbackFuture = new CallbackFuture<Cursor>() {
                        @Override
                        public void onSuccess(Cursor cursor) {
                            super.onSuccess(cursor);
                            tryForce(journal.checkpoint(cursor));
                        }
                    };
                    journal.append(entry, callbackFuture);
                    return VOID;
                }
            };
        }
    };

    public static final Void VOID = null;

    protected Checkpoint last = new Checkpoint(0L);

    protected void tryForce(Checkpoint checkpoint) {
        if (checkpoint.compareTo(last) > 0) {
            actor(Updatable.class).force(checkpoint);
            last = checkpoint;
        }
    }

    public abstract Callable<Void> append(Journal journal, Entry<Md5Key, byte[]> entry);
}
