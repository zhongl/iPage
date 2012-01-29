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

import com.github.zhongl.ex.journal.Closable;
import com.github.zhongl.ex.journal.OverflowCallback;

/**
 * {@link com.github.zhongl.ex.page.Page} is a high level abstract entity focus on IO manipulation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public interface Page extends Closable {

    /**
     * Commit group to page.
     *
     * @param group    to appending.
     * @param force    to driver if it is true.
     * @param callback for appending overflow.
     */
    void commit(Group group, boolean force, OverflowCallback callback);

    /** Delete bytes of page on the driver. */
    void delete();

    Group newGroup();

}
