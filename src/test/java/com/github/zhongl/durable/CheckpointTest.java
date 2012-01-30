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

package com.github.zhongl.durable;

import com.github.zhongl.journal.Event;
import com.github.zhongl.page.Page;
import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CheckpointTest extends FileTestContext {

    @Test
    public void initialize() throws Exception {
        dir = testDir("initialize");
        dir.delete();
        Checkpoint checkpoint = new Checkpoint(dir, 16);
        assertThat(checkpoint.lastCursor(), is(Cursor.head()));
    }

    @Test
    public void loadLast() throws Exception {
        dir = testDir("loadLast");

        new File(dir, "0.4096").createNewFile();
        Checkpoint checkpoint = new Checkpoint(dir, 16);
        assertThat(checkpoint.lastCursor(), is(new Cursor(4096L)));
    }

    @Test
    public void savePoint() throws Exception {
        dir = testDir("savePoint");

        Checkpoint checkpoint = new Checkpoint(dir, 16);
        Cursor cursor0 = new Cursor(0L);
        assertThat(checkpoint.canSave(cursor0), is(false));
        checkpoint.save(0L, cursor0);

        Cursor cursor1 = new Cursor(16L);
        assertThat(checkpoint.canSave(cursor1), is(true));
        checkpoint.save(1L, cursor1);
        assertThat(checkpoint.lastCursor(), is(cursor1));
        assertThat(new File(dir, "1.16").exists(), is(true));

        Cursor cursor2 = new Cursor(30L);
        assertThat(checkpoint.canSave(cursor2), is(false));

        Cursor cursor3 = new Cursor(32L);
        assertThat(checkpoint.canSave(cursor3), is(true));
        checkpoint.save(3L, cursor3);
        assertThat(checkpoint.lastCursor(), is(cursor3));
        assertThat(new File(dir,"1.16").exists(),is(false));
        assertThat(new File(dir,"3.32").exists(),is(true));

    }

    @Test
    public void loadLastWithMoreThanOneSavePoints() throws Exception {
        dir = testDir("loadLast");

        new File(dir, "0.4096").createNewFile();
        new File(dir, "4.10240").createNewFile();
        Checkpoint checkpoint = new Checkpoint(dir, 16);
        assertThat(checkpoint.lastCursor(), is(new Cursor(10240L)));

        Page<Event> page = mock(Page.class);

        when(page.number()).thenReturn(4L);
        assertThat(checkpoint.isApplied(page), is(true));
        when(page.number()).thenReturn(5L);
        assertThat(checkpoint.isApplied(page), is(false));
    }


}
