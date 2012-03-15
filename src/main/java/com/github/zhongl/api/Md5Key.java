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

package com.github.zhongl.api;

import com.github.zhongl.index.Key;
import com.github.zhongl.util.Md5;

import javax.annotation.concurrent.ThreadSafe;
import java.math.BigInteger;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Md5Key implements Key {

    public static final int BYTE_LENGTH = 16;

    private final byte[] bytes;

    final BigInteger bigInteger;

    public Md5Key(byte[] bytes) {
        checkArgument(bytes.length == BYTE_LENGTH, "Invalid bytes length %s", bytes.length);
        this.bytes = bytes;
        bigInteger = new BigInteger(1, bytes);
    }

    public byte[] toBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return Md5.toHex(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Md5Key md5Key = (Md5Key) o;
        return Arrays.equals(bytes, md5Key.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(Key that) {
        checkArgument(that.getClass() == getClass(), "Expect %s but %s", getClass(), that.getClass());
        if (this == that) return 0;
        return bigInteger.compareTo(((Md5Key) that).bigInteger);
    }

}
