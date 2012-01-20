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


import com.google.common.base.Function;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closable {
    private final Pages pages;

    public Journal(Pages pages) {this.pages = pages;}

    /**
     * Append event.
     *
     * @param event is the instance of type which compound {@link com.github.zhongl.codec.Codec}s supported.
     * @param force event to driver for duration if it's true.
     *
     * @return last offset.
     */
    public long append(Object event, boolean force) {
        Record record = pages.append(event, force);
        return record.offset() + record.length();
    }

    /**
     * Erase events before the offset.
     *
     * @param offset of journal.
     */
    public void erase(long offset) {
        pages.range().tail(offset).remove();
    }

    /**
     * Recover unapplied events to {@link com.github.zhongl.journal1.Applicable}.
     *
     * @param applicable {@link com.github.zhongl.journal1.Applicable}
     */
    public void recover(final Applicable applicable) {
        try {
            pages.range().foreach(new Function<Record, Void>() {
                @Override
                public Void apply(@Nullable Record record) {
                    applicable.apply(record);
                    return null;
                }
            });
        } catch (IllegalStateException ignored) { /* invalidChecksum */ }

        applicable.force();
        pages.reset();
    }

    @Override
    public void close() {
        pages.close();
    }

}
