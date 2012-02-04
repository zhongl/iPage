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
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class JournalTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");

        Journal journal = new Journal(dir, new StringCodec());

        journal.append("1", false);

        Revision revision = journal.append("2", true);

        journal.eraseBy(revision);

        journal.append("3", true);

        journal.close(); // mock crash

        journal = new Journal(dir, new StringCodec());

        Applicable applicable = mock(Applicable.class);

        doReturn(revision).when(applicable).lastCheckpoint();

        journal.recover(applicable);

        verify(applicable).apply("3");

        journal.close();
    }

}
