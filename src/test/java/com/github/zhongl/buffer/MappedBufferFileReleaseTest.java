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

package com.github.zhongl.buffer;

import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappedBufferFileReleaseTest extends FileBase {

    private MappedBufferFile writeable;
    private MappedBufferFile readOnly;
    private MappedBufferFile releaseTrigger;

    @Override
    @Before
    public void setUp() throws Exception {
        MappedBufferFile.aliveFiles.clear();
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        writeable.release();
        readOnly.release();
        releaseTrigger.release();
    }

    @Test
    public void tryRelease() throws Exception {
        dir = testDir("tryRelease");
        dir.mkdirs();
        long maxIdleTimeMIllis = 10L;
        writeable = writeable("0");
        readOnly = readable("1", maxIdleTimeMIllis);
        assertThat(writeable.isReleased(), is(false));
        assertThat(readOnly.isReleased(), is(false));

        Thread.sleep(10L);
        releaseTrigger = readable("2", maxIdleTimeMIllis);

        assertThat(writeable.isReleased(), is(false));
        assertThat(readOnly.isReleased(), is(true));
    }

    @Test
    public void forceRelease() throws Exception {
        dir = testDir("forceRelease");
        dir.mkdirs();
        long maxIdleTimeMIllis = 10L;
        writeable = writeable("0");
        readOnly = readable("1", maxIdleTimeMIllis);
        DirectBufferMapper mapper = mock(DirectBufferMapper.class);
        doThrow(new OutOfMemoryError()).when(mapper).map();
        assertThat(writeable.isReleased(), is(false));
        assertThat(readOnly.isReleased(), is(false));
        Thread.sleep(1L);
        try {
            releaseTrigger = new MappedBufferFile(mapper, maxIdleTimeMIllis);
            releaseTrigger.readBy(CommonAccessors.BYTES, 0, 1); // trigger buffer mapping
            fail("OutOfMemoryError should be throwed.");
        } catch (OutOfMemoryError e) {}
        assertThat(writeable.isReleased(), is(false));
        assertThat(readOnly.isReleased(), is(true));
    }

    private MappedBufferFile readable(String name, long maxIdleTimeMIllis) throws IOException {
        File toRead = new File(dir, name);
        Files.write(Longs.toByteArray(0L), toRead);
        MappedBufferFile bufferFile = MappedBufferFile.readOnly(toRead, maxIdleTimeMIllis);
        bufferFile.readBy(CommonAccessors.LONG, 0, 8);
        return bufferFile;
    }

    private MappedBufferFile writeable(String name) throws IOException {
        MappedBufferFile writeable = MappedBufferFile.writeable(new File(dir, name), 4096);
        writeable.writeBy(CommonAccessors.BYTES, 0, "value".getBytes()); // trigger buffer mapping
        return writeable;
    }
}
