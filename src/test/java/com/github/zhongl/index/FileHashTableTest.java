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

package com.github.zhongl.index;

import com.github.zhongl.integerity.Validator;
import com.github.zhongl.ipage.OverflowException;
import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileHashTableTest extends FileBase {

    private FileHashTable fileHashTable;

    @Override
    public void tearDown() throws Exception {
        if (fileHashTable != null) fileHashTable.close();
        super.tearDown();
    }

    @Test
    public void useDefaultSizeIfInvalidBucketSizeWereSet() throws Exception {
        file = testFile("invalidBucketSize");
        assertThat(new FileHashTable(file, -1).amountOfBuckets(), is(256));
    }

    @Test
    public void putAndGet() throws Exception {
        file = testFile("putAndGet");
        fileHashTable = new FileHashTable(file, 1);

        Md5Key key = Md5Key.generate("key".getBytes());

        long offset = 7L;
        assertThat(fileHashTable.put(key, offset), is(nullValue()));
        assertThat(fileHashTable.get(key), is(offset));

        fileHashTable.close();

        fileHashTable = new FileHashTable(file, 1);
        assertThat(fileHashTable.get(key), is(offset)); // assert sync to file
    }

    @Test
    public void remove() throws Exception {
        file = testFile("remove");
        fileHashTable = new FileHashTable(file, 1);

        Md5Key key = Md5Key.generate("key".getBytes());
        Long offset = 7L;
        assertThat(fileHashTable.put(key, offset), is(nullValue()));
        assertThat(fileHashTable.remove(key), is(offset));
        assertThat(fileHashTable.get(key), is(nullValue()));
    }

    @Test
    public void validateOrRecoveryIfNoBroken() throws Exception {
        file = testFile("validateOrRecoveryIfNoBroken");

        fileHashTable = new FileHashTable(file, 1);
        Validator<Slot, IOException> validator = mock(Validator.class);
        assertThat(fileHashTable.validateOrRecoverBy(validator), is(true));// validateAndRecoverBy a empty bucket

        fileHashTable.put(Md5Key.generate("key".getBytes()), 7L);
        fileHashTable.close();
        fileHashTable = new FileHashTable(file, 1);
        doReturn(true).when(validator).validate(any(Slot.class));
        assertThat(fileHashTable.validateOrRecoverBy(validator), is(true)); // validateAndRecoverBy a exist bucket
    }

    @Test
    public void validateOrRecoveryIfSlotBroken() throws Exception {
        file = testFile("validateOrRecoveryIfSlotBroken");
        byte[] md5Bytes0 = DigestUtils.md5("value0");
        byte[] md5Bytes1 = DigestUtils.md5("value1");
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
        Files.write(brokenBucketContent, file); // mock broken fileHashTable file.

        fileHashTable = new FileHashTable(file, 1);

        Validator<Slot, IOException> validator = new Validator<Slot, IOException>() {
            @Override
            public boolean validate(Slot slot) throws IOException {
                return slot.offset() == 4L;
            }
        };
        assertThat(fileHashTable.validateOrRecoverBy(validator), is(false));
    }

    @Test
    public void putInReleasedSlot() throws Exception {
        file = testFile("putInReleasedSlot");
        fileHashTable = new FileHashTable(file, 1);

        fillFullBuckets();

        Md5Key key0 = Md5Key.generate(Ints.toByteArray(0));
        Md5Key key1 = Md5Key.generate(Ints.toByteArray(1));
        fileHashTable.remove(key0);
        fileHashTable.remove(key1);
        assertThat(fileHashTable.put(key1, 7L), is(nullValue()));
        assertThat(fileHashTable.put(key0, 7L), is(nullValue())); // no exception means new item index put in released slot.
    }

    @Test
    public void getAndRemoveEmplyBucket() throws Exception {
        file = testFile("getAndRemoveEmplyBucket");
        fileHashTable = new FileHashTable(file, 1);

        Md5Key key = Md5Key.generate(Ints.toByteArray(1));
        assertThat(fileHashTable.get(key), is(nullValue()));
        assertThat(fileHashTable.remove(key), is(nullValue()));
    }

    @Test
    public void getAndRemoveByInvalidKey() throws Exception {
        file = testFile("getAndRemoveByInvalidKey");
        fileHashTable = new FileHashTable(file, 1);

        fillFullBuckets();

        Md5Key invalidKey = Md5Key.generate(Ints.toByteArray(163));

        assertThat(fileHashTable.get(invalidKey), is(nullValue()));
        assertThat(fileHashTable.remove(invalidKey), is(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void unknownSlotState() throws Exception {
        file = testFile("unknownSlotState");
        fileHashTable = new FileHashTable(file, 1);
        Files.write(new byte[] {3}, file);  // write a unknown slot state
        fileHashTable.get(Md5Key.generate(Ints.toByteArray(1))); // trigger exception
    }

    @Test(expected = OverflowException.class)
    public void noSlotForNewItemIndex() throws Exception {
        file = testFile("noSlotForNewItemIndex");
        fillFullBuckets();
        Md5Key key163 = Md5Key.generate(Ints.toByteArray(163));
        fileHashTable.put(key163, 7L);  // trigger exception
    }

    @Test
    public void replace() throws Exception {
        file = testFile("replace");
        fileHashTable = new FileHashTable(file, 1);
        Md5Key key = Md5Key.generate("key".getBytes());
        assertThat(fileHashTable.put(key, 7L), is(nullValue()));
        assertThat(fileHashTable.put(key, 4L), is(7L));
    }

    @Test
    public void cleanup() throws Exception {
        file = testFile("cleanup");
        fileHashTable = new FileHashTable(file, 1);
        Md5Key key = Md5Key.generate("key".getBytes());
        fileHashTable.remove(key); // test remove a non-exist key with no effect
        fileHashTable.put(key, 7L);
        fileHashTable.put(key, 7L);
        fileHashTable.remove(key);
        assertThat(fileHashTable.isEmpty(), is(true));
        fileHashTable.clean();
        assertThat(file.exists(), is(false));
    }

    @Test(expected = IllegalStateException.class)
    public void putAfterClose() throws Exception {
        file = testFile("putAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        ;
        fileHashTable.put(null, null);
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterClose() throws Exception {
        file = testFile("getAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.get(null);
    }

    @Test(expected = IllegalStateException.class)
    public void removeAfterClose() throws Exception {
        file = testFile("removeAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.remove(null);
    }

    @Test(expected = IllegalStateException.class)
    public void sizeAfterClose() throws Exception {
        file = testFile("sizeAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.size();
    }

    @Test(expected = IllegalStateException.class)
    public void bucketsAfterClose() throws Exception {
        file = testFile("bucketsAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.amountOfBuckets();
    }

    @Test(expected = IllegalStateException.class)
    public void flushAfterClose() throws Exception {
        file = testFile("flushAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.flush();
    }

    @Test(expected = IllegalStateException.class)
    public void isEmptyAfterClose() throws Exception {
        file = testFile("isEmptyAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void fileAfterClose() throws Exception {
        file = testFile("fileAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.file();
    }

    @Test(expected = IllegalStateException.class)
    public void validateOrRecoverAfterClose() throws Exception {
        file = testFile("validateOrRecoverAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.validateOrRecoverBy(null);
    }

    @Test
    public void closeAfterClose() throws Exception {
        file = testFile("closeAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.close();
    }

    @Test
    public void cleanAfterClose() throws Exception {
        file = testFile("cleanAfterClose");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.close();
        fileHashTable.clean();
    }

    @Test(expected = IllegalStateException.class)
    public void putAfterClean() throws Exception {
        file = testFile("putAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.put(null, null);
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterClean() throws Exception {
        file = testFile("getAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.get(null);
    }

    @Test(expected = IllegalStateException.class)
    public void removeAfterClean() throws Exception {
        file = testFile("removeAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.remove(null);
    }

    @Test(expected = IllegalStateException.class)
    public void sizeAfterClean() throws Exception {
        file = testFile("sizeAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.size();
    }

    @Test(expected = IllegalStateException.class)
    public void bucketsAfterClean() throws Exception {
        file = testFile("bucketsAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.amountOfBuckets();
    }

    @Test(expected = IllegalStateException.class)
    public void flushAfterClean() throws Exception {
        file = testFile("flushAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.flush();
    }

    @Test(expected = IllegalStateException.class)
    public void isEmptyAfterClean() throws Exception {
        file = testFile("isEmptyAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void fileAfterClean() throws Exception {
        file = testFile("fileAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.file();
    }

    @Test(expected = IllegalStateException.class)
    public void validateOrRecoverAfterClean() throws Exception {
        file = testFile("validateOrRecoverAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.validateOrRecoverBy(null);
    }

    @Test
    public void closeAfterClean() throws Exception {
        file = testFile("closeAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.close();
    }

    @Test
    public void cleanAfterClean() throws Exception {
        file = testFile("cleanAfterClean");

        fileHashTable = new FileHashTable(file, 1);
        fileHashTable.clean();
        fileHashTable.clean();
    }

    @Test
    public void fileHashTableToString() throws Exception {
        file = testFile("fileHashTableToString");
        String expect = "FileHashTable{amountOfBuckets=1, " +
                "file=target/tmpTestFiles/FileHashTableTest.fileHashTableToString, " +
                "occupiedSlots=0, " +
                "cleaned=false, " +
                "closed=false}";
        assertThat(new FileHashTable(file, 1).toString(), is(expect));
    }

    private void fillFullBuckets() throws Exception {
        fileHashTable = new FileHashTable(file, 1);
        for (int i = 0; i < 163; i++) {
            fileHashTable.put(Md5Key.generate(Ints.toByteArray(i)), 10L);
        }
    }

}
