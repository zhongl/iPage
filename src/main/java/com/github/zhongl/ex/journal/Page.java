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

package com.github.zhongl.ex.journal;

import com.github.zhongl.ex.nio.Closable;

import java.nio.ByteBuffer;

/**
 * {@link com.github.zhongl.ex.journal.Page} is a high level abstract entity focus on IO manipulation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public interface Page extends Closable, Comparable<Long> {

    public long offset();

    public int length();

    /**
     * Append buffer to page.
     *
     * @param buffer   to appending.
     * @param force    to driver if it is true.
     * @param callback for appending overflow.
     *
     * @return offset after appended.
     */
    public int append(ByteBuffer buffer, boolean force, OverflowCallback callback);

    /**
     * Slice a readonly {@link java.nio.ByteBuffer} by offset and length.
     *
     * @param offset of buffer in page.
     * @param length of buffer in page.
     *
     * @return readonly {@link java.nio.ByteBuffer}
     */
    public ByteBuffer slice(int offset, int length);

    /** Delete bytes of page on the driver. */
    public void delete();
}
