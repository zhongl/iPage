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

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Checkpoint;
import com.github.zhongl.ex.journal.Journal;
import com.github.zhongl.ex.util.CallByCountOrElapse;
import com.github.zhongl.ex.util.Entry;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
class DefaultRecorder extends Actor implements Recorder, Erasable {

    static final byte[] NULL_VALUE = new byte[0];

    private final QuanlityOfService quanlityOfService;
    private final Journal journal;
    private final CallByCountOrElapse callByCountOrElapse;
    private final FlowControllor controllor;

    public DefaultRecorder(
            Journal journal,
            QuanlityOfService quanlityOfService,
            FlowControllor controllor,
            int forceCount,
            long forceMilliseconds
    ) throws IOException {
        super(forceMilliseconds / 2);

        this.controllor = controllor;
        this.quanlityOfService = quanlityOfService;
        this.journal = journal;
        this.callByCountOrElapse = new CallByCountOrElapse(forceCount, forceMilliseconds, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                DefaultRecorder.this.journal.force();
                return null;
            }
        });
    }

    @Override
    public boolean append(Md5Key key, byte[] value) {
        checkArgument(checkNotNull(value).length > 0);
        return append(new Entry<Md5Key, byte[]>(checkNotNull(key), value));
    }

    @Override
    public boolean remove(Md5Key key) {
        return append(new Entry<Md5Key, byte[]>(checkNotNull(key), NULL_VALUE));
    }

    @Override
    public void erase(final Checkpoint checkpoint) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                journal.erase(checkpoint);
                return null;
            }
        });
    }

    @Override
    protected void hearbeat() {
        try {
            callByCountOrElapse.tryCallByElapse();
        } catch (Exception e) {
            haltByUnexpected(e);
        }
    }

    private void haltByUnexpected(Exception e) {
        e.printStackTrace();
        controllor.halt();
        stop();
    }

    private boolean append(final Entry<Md5Key, byte[]> entry) {
        try {
            controllor.call(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    submit(quanlityOfService.append(journal, entry)).get();
                    callByCountOrElapse.tryCallByCount();
                    return null;
                }
            });

            return true;
        } catch (Exception e) {
            haltByUnexpected(e);
            return false;
        }
    }

}
