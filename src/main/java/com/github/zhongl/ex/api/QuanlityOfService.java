package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.CallbackFuture;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Journal;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;

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
                    CallbackFuture<Revision> callbackFuture = new CallbackFuture<Revision>() {
                        @Override
                        public void onSuccess(Revision revision) {
                            super.onSuccess(revision);
                            actor(Updatable.class).update(this,entry);
                        }
                    };
                    journal.append(entry, callbackFuture);
                    callbackFuture.get();

                    return null;
                }
            };
        }
    }, BALANCE {
        @Override
        public Callable<Void> append(final Journal journal, final Entry<Md5Key, byte[]> entry) {
            return new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    CallbackFuture<Revision> callbackFuture = new CallbackFuture<Revision>();
                    journal.append(entry, callbackFuture);
                    actor(Updatable.class).update(callbackFuture,entry);
                    callbackFuture.get();

                    return null;
                }
            };
        }
    }, LATENCY {
        @Override
        public Callable<Void> append(final Journal journal, final Entry<Md5Key, byte[]> entry) {
            return new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    CallbackFuture<Revision> callbackFuture = new CallbackFuture<Revision>();
                    journal.append(entry, callbackFuture);
                    actor(Updatable.class).update(callbackFuture,entry);

                    return null;
                }
            };
        }
    };

    public abstract Callable<Void> append(Journal journal, Entry<Md5Key, byte[]> entry);
}
