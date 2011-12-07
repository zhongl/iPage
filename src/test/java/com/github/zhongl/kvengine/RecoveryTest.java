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

package com.github.zhongl.kvengine;

import com.github.zhongl.index.Index;
import com.github.zhongl.ipage.IPage;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RecoveryTest {

    @Test(expected = IllegalStateException.class)
    public void runFailureBecauseOfIOException() throws Exception {
        Index index = mock(Index.class);
        IPage iPage = mock(IPage.class);
        doThrow(new IOException()).when(index).recoverBy(any(Recovery.RecordFinder.class));
        new Recovery(index, iPage).run();
    }

}
