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

package com.github.zhongl.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Md5 {
    private static final char[] HEX_DIGITS = {'0', '1', '2', '3',
            '4', '5', '6', '7',
            '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f'};

    private Md5() { }

    public static byte[] md5(byte[] bytes) {
        return messageDigest().digest(bytes);
    }

    public static MessageDigest messageDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toHex(byte[] bytes) {
        char[] chars = new char[HEX_DIGITS.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            chars[i * 2] = HEX_DIGITS[bytes[i] >>> 4 & 0xf];
            chars[i * 2 + 1] = HEX_DIGITS[bytes[i] & 0xf];
        }
        return new String(chars);
    }

}
