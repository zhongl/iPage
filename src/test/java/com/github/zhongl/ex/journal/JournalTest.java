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

package com.github.zhongl.ex.journal;

import com.github.zhongl.ex.codec.StringCodec;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.github.zhongl.util.FileTestContext;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;

import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class JournalTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");

        Journal journal = new Journal(dir, 26, new StringCodec());

        FutureCallback<Cursor> ignore = FutureCallbacks.ignore();

        journal.append("1", ignore);
        journal.append("2", ignore);
        journal.force();

        CallbackFuture<Cursor> forceCallback = new CallbackFuture<Cursor>();
        journal.append("3", forceCallback);
        journal.force();
        Checkpoint checkpoint = journal.checkpoint(forceCallback.get());

        journal.erase(checkpoint);

        journal.close(); // mock crash

        journal = new Journal(dir, 28, new StringCodec());

        Applicable applicable = mock(Applicable.class);

        doReturn(checkpoint).when(applicable).lastCheckpoint();

        journal.recover(applicable);

        verify(applicable).apply("3");


        journal.close();
    }

}
