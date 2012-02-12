/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ipage;

import com.github.zhongl.util.Md5;

import javax.annotation.concurrent.ThreadSafe;
import java.math.BigInteger;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Key extends Number<Key> {

    public static final int BYTE_LENGTH = 16;

    private final byte[] md5;

    final BigInteger bigInteger;

    public Key(String hex) {
        checkArgument(hex.length() == BYTE_LENGTH * 2, "Invalid md5 string length %s", hex.length());
        this.bigInteger = new BigInteger(hex, 16);
        this.md5 = toByteArray(bigInteger);
    }

    public Key(BigInteger bigInteger) {
        this.bigInteger = bigInteger;
        this.md5 = toByteArray(bigInteger);
    }

    public Key(byte[] md5) {
        checkArgument(md5.length == BYTE_LENGTH, "Invalid md5 bytes length %s", md5.length);
        this.md5 = md5;
        bigInteger = new BigInteger(1, md5);
    }

    public byte[] bytes() {
        return md5;
    }

    @Override
    public String toString() {
        return Md5.toHex(md5);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key md5Key = (Key) o;
        return Arrays.equals(md5, md5Key.md5);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(md5);
    }

    @Override
    public int compareTo(Key o) {
        return bigInteger.compareTo(o.bigInteger);
    }

    private byte[] toByteArray(BigInteger bigInteger) {
        byte[] bytes = bigInteger.toByteArray();
        if (bytes.length == BYTE_LENGTH) return bytes;
        if (bytes.length > BYTE_LENGTH) return Arrays.copyOfRange(bytes, 1, BYTE_LENGTH + 1);
        byte[] result = new byte[BYTE_LENGTH];
        System.arraycopy(bytes, 0, result, result.length - bytes.length, bytes.length);
        return result;
    }
}
