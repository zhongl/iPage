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

package com.github.zhongl.index;

import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexTest extends FileBase {

    private Index index;

    @After
    public void tearDown() throws Exception {
        if (index != null) index.close();
    }

    @Test
    public void grow() throws Exception {
        dir = testDir("grow");
        newIndex();

        Cursor cursor = new Cursor(7L);
        for (int i = 0; i < 163; i++) {
            index.put(Md5Key.generate(Ints.toByteArray(i)), cursor);
        }

        assertExistFile("0");
        assertNotExistFile("1");

        index.put(Md5Key.generate(Ints.toByteArray(163)), cursor);

        assertExistFile("0");
        assertExistFile("1");
    }

    @Test
    public void autoremoveMigratedBuckets() throws Exception {
        dir = testDir("autoremoveMigratedBuckets");
        newIndex();

        Cursor cursor = new Cursor(7L);
        for (int i = 0; i < 164; i++) {
            index.put(Md5Key.generate(Ints.toByteArray(i)), cursor);
        }

        assertExistFile("0");
        assertExistFile("1");

        for (int i = 0; i < 164; i++) {
            index.get(Md5Key.generate(Ints.toByteArray(i)));
        }

        assertNotExistFile("0");
        assertExistFile("1");
    }

    @Test
    public void removeFromExtendedIndex() throws Exception {
        dir = testDir("removeFromExtendedIndex");
        newIndex();
        Cursor cursor = new Cursor(7L);
        for (int i = 0; i < 164; i++) {
            index.put(Md5Key.generate(Ints.toByteArray(i)), cursor);
        }
        assertThat(index.remove(Md5Key.generate(Ints.toByteArray(163))), is(cursor)); // remove from index file 1
        assertThat(index.remove(Md5Key.generate(Ints.toByteArray(162))), is(cursor)); // remove from index file 0
        assertThat(index.remove(Md5Key.generate(Ints.toByteArray(164))), is(nullValue())); // remove from index file 0
    }

    @Test
    public void removeNonExistKey() throws Exception {
        dir = testDir("removeNonExistKey");
        newIndex();
        assertThat(index.remove(Md5Key.generate(Ints.toByteArray(0))), is(nullValue())); // remove from empty index
        index.put(Md5Key.generate("key".getBytes()), new Cursor(7L));
        assertThat(index.remove(Md5Key.generate(Ints.toByteArray(1))), is(nullValue())); // remove from non-empty index
    }

    @Test
    public void getNonExsitKey() throws Exception {
        dir = testDir("getNonExsitKey");
        newIndex();
        assertThat(index.get(Md5Key.generate(Ints.toByteArray(0))), is(nullValue()));
    }

    @Test
    public void loadExist() throws Exception {
        dir = testDir("loadExist");
        newIndex();

        Cursor cursor = new Cursor(7L);
        for (int i = 0; i < 164; i++) {
            index.put(Md5Key.generate(Ints.toByteArray(i)), cursor);
        }

        index.close();

        assertThat(new File(dir, "0").length(), is(4096L));
        assertThat(new File(dir, "1").length(), is(8192L));

        newIndex();

        assertThat(index.get(Md5Key.generate(Ints.toByteArray(0))), is(cursor));     // migrate
        assertThat(index.get(Md5Key.generate(Ints.toByteArray(162))), is(cursor));   // migrate
        assertThat(index.get(Md5Key.generate(Ints.toByteArray(163))), is(cursor));
    }

    @Test
    public void validateOrRecoveryIfNoBroken() throws Exception {
        dir = testDir("validateOrRecoveryIfNoBroken");
        newIndex();

        Cursor cursor = new Cursor(7L);
        for (int i = 0; i < 164; i++) {
            index.put(Md5Key.generate(Ints.toByteArray(i)), cursor);
        }

        index.flush();
        index.close();

        newIndex();

        assertThat(index.validateOrRecoverBy(null), is(true));
    }

    @Test
    public void validateOrRecoveryIfHasBroken() throws Exception {
        dir = testDir("validateOrRecoveryIfHasBroken");

        byte[] md5Bytes0 = Md5Key.md5("value0".getBytes());
        byte[] md5Bytes1 = Md5Key.md5("value1".getBytes());
        byte[] bucketCRC = Longs.toByteArray(0);
        byte[] brokenBucketContent = Bytes.concat(
                new byte[] {1},
                md5Bytes0,
                Longs.toByteArray(4L),
                new byte[] {1},
                md5Bytes1,
                Longs.toByteArray(7L),
                new byte[4038],
                bucketCRC
        );
        dir.mkdirs();
        Files.write(brokenBucketContent, new File(dir, 0 + "")); // mock broken fileHashTable file.

        newIndex();

        Validator validator = new Validator() {
            @Override
            public boolean validate(Md5Key value, Cursor cursor) throws IOException {
                return true;
            }
        };

        assertThat(index.validateOrRecoverBy(validator), is(false));
    }

    private void newIndex() throws IOException {
        index = new Index(dir, 1);
    }


}
