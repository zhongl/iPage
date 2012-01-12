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

package com.github.zhongl.journal1;


import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * A {@link com.github.zhongl.journal1.Journal} can have only one {@link com.github.zhongl.journal1.Event} consumer.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
public  class Journal implements Closeable {

    public static Journal open(File dir, EventLoader loader) {
        return null;  // TODO open
    }

    public void append(Event event) {
        // TODO append
    }

    public Event headEvent() {
        return null;  // TODO headEvent
    }

    public void removeHeadEvent() {
        // TODO removeHeadEvent
    }

    @Override
    public void close() throws IOException {
        // TODO close
    }
}
