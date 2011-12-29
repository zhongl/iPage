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

package com.github.zhongl.journal;

import org.junit.Test;

import java.nio.channels.FileChannel;

import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest {


    @Test
    public void addAndFix() throws Exception {
        FileChannel fileChannel = mock(FileChannel.class);
        int minimizeLength = 10;
        Page page = new Page(fileChannel, minimizeLength);

        Event mock = mock(Event.class);
        page.add(mock);
        // TODO addAndFix 
    }

}
