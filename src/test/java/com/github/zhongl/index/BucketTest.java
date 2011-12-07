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
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BucketTest {

    @Test
    public void validateOrRecoveryIfOnlyCRCBroken() throws Exception {
        byte[] md5Bytes0 = DigestUtils.md5("value0");
        byte[] md5Bytes1 = DigestUtils.md5("value1");
        byte[] brokenCRC = Longs.toByteArray(0);
        byte[] brokenBucketContent = Bytes.concat(
                new byte[] {1},
                md5Bytes0,
                Longs.toByteArray(4L),
                new byte[] {1},
                md5Bytes1,
                Longs.toByteArray(7L),
                new byte[4038],
                brokenCRC
        );
        Bucket bucket = new Bucket(ByteBuffer.wrap(brokenBucketContent));// mock broken fileHashTable file.

        Validator<Slot> validator = new Validator<Slot>() {
            @Override
            public boolean validate(Slot slot) {
                return slot.offset() < 10L;
            }
        };
        assertThat(bucket.validateOrRecoverBy(validator), is(true));
    }

}
