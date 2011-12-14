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
import org.junit.Test;

import java.io.IOException;

import static com.github.zhongl.buffer.CommonAccessors.BYTE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class MappedBufferFileTest extends FileBase {

    private MappedBufferFile mappedBufferFile;

    @Override
    public void tearDown() throws Exception {
        mappedBufferFile.release();
        super.tearDown();
    }

    @Test
    public void read() throws Exception {
        file = testFile("read");
        newMappedBufferFile();
        Files.write(new byte[] {7}, file); // fake a file
        Byte b = mappedBufferFile.readBy(BYTE, 0, 1);
        assertThat(b, is((byte) 7));
    }

    @Test
    public void write() throws Exception {
        file = testFile("write");
        newMappedBufferFile();
        int wrote = mappedBufferFile.writeBy(BYTE, 0, (byte) 1);
        assertThat(wrote, is(1));
    }

    private void newMappedBufferFile() throws IOException {
        mappedBufferFile = new MappedBufferFile(new DirectBufferMapper(file, 4096, false), Long.MAX_VALUE);
    }
}
