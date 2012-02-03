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

package com.github.zhongl.ex.index;

import com.github.zhongl.ex.page.Number;

import javax.annotation.concurrent.ThreadSafe;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Md5Key extends Number<Md5Key> {

    public static final int BYTE_LENGTH = 16;

    private static final char HEX_DIGITS[] = {'0', '1', '2', '3',
            '4', '5', '6', '7',
            '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f'};

    public static final Md5Key MIN = new Md5Key(new byte[] {
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00
    });

    public static final Md5Key MAX = new Md5Key(new byte[] {
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff
    });

    public static byte[] md5(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            return digest.digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static Md5Key generate(byte[] bytes) {
        return new Md5Key(md5(bytes));
    }

    public static Md5Key generate(String s) {
        return generate(s.getBytes());
    }

    private final byte[] md5;

    final BigInteger bigInteger;

    public Md5Key(String hex) {
        checkArgument(hex.length() == BYTE_LENGTH * 2, "Invalid md5 string length %s", hex.length());
        this.bigInteger = new BigInteger(hex, 16);
        this.md5 = Arrays.copyOfRange(bigInteger.toByteArray(), 1, 17);
    }

    public Md5Key(BigInteger bigInteger) {
        this.bigInteger = bigInteger;
        this.md5 = Arrays.copyOfRange(bigInteger.toByteArray(), 1, 17);
    }

    public Md5Key(byte[] md5) {
        checkArgument(md5.length == BYTE_LENGTH, "Invalid md5 bytes length %s", md5.length);
        this.md5 = md5;
        bigInteger = new BigInteger(1, md5);
    }

    public byte[] bytes() {
        return md5;
    }

    @Override
    public String toString() {
        char[] chars = new char[BYTE_LENGTH * 2];
        for (int i = 0; i < md5.length; i++) {
            chars[i * 2] = HEX_DIGITS[md5[i] >>> 4 & 0xf];
            chars[i * 2 + 1] = HEX_DIGITS[md5[i] & 0xf];
        }
        return new String(chars);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Md5Key md5Key = (Md5Key) o;
        return Arrays.equals(md5, md5Key.md5);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(md5);
    }

    @Override
    public int compareTo(Md5Key o) {
        return bigInteger.compareTo(o.bigInteger);
    }
}
