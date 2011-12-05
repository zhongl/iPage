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

package com.github.zhongl.ipage;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class Recovery implements Runnable {

    private final Index index;
    private final IPage ipage;

    public Recovery(Index index, IPage ipage) {
        this.index = index;
        this.ipage = ipage;
    }

    @Override
    public void run() {
        try {
            ipage.recover();
            index.recoverBy(new InnerRecordFinder());
        } catch (IOException e) {
            throw new IllegalStateException("Can't run recovery, because:", e);
        }
    }

    public interface RecordFinder {
        Record getRecordIn(long offset) throws IOException;
    }

    class InnerRecordFinder implements RecordFinder {
        @Override
        public Record getRecordIn(long offset) throws IOException {
            try {
                return ipage.get(offset);
            } catch (IllegalArgumentException e) {
                return null;// offset or length is illegal
            }
        }
    }

}
