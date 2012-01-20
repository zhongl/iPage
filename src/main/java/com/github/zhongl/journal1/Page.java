/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.journal1;

import com.github.zhongl.codec.Codec;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
abstract class Page implements Closable {

    protected final File dir;
    protected final Codec codec;
    protected final int pageCapacity;
    protected final long number;
    protected final DeletingCallback deletingCallback;

    protected Page(File dir, Codec codec, int pageCapacity, long number, DeletingCallback deletingCallback) {
        this.dir = dir;
        this.codec = codec;
        this.pageCapacity = pageCapacity;
        this.number = number;
        this.deletingCallback = deletingCallback;
    }

    public abstract Record append(Object object, boolean force, OverflowCallback<Object> overflowCallback);

    public abstract List<Record> append(List<Object> objects, boolean force, OverflowCallback<List<Object>> overflowCallback);

    public Range range() {
        return new InnerRange(number, number + length());
    }

    protected abstract long length();

    protected void delete() {
        if (deleted()) return;
        implDelete();
        deletingCallback.onDelete(Page.this);
    }

    protected abstract boolean deleted();

    protected abstract void implDelete();

    protected abstract Record record(long offset);

    private class InnerRange extends Range {

        protected InnerRange(long head, long tail) {
            super(head, tail);
        }

        @Override
        public Record record(long offset) {
            checkState(!deleted(), "Page has already deleted.");
            return Page.this.record(offset);
        }

        @Override
        public Range head(long offset) {
            checkArgument(offset > head() && offset < tail());
            return new InnerRange(offset, tail());
        }

        @Override
        public Range tail(long offset) {
            checkArgument(offset > head() && offset < tail());
            return new InnerRange(head(), tail());
        }

        @Override
        public void remove() {
            delete();
        }

    }
}
