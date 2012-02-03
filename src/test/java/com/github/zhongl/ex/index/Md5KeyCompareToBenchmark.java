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

package com.github.zhongl.ex.index;

import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;

import static com.github.zhongl.util.Benchmarks.benchmark;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Md5KeyCompareToBenchmark {

    static final int SIZE = 10000;
    private Md5Key[] keys;

    @Before
    public void setUp() throws Exception {
        keys = new Md5Key[SIZE];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = Md5Key.generate(i + "");
        }
    }

    @Test
    public void string() throws Exception {
        final String[] strings = new String[SIZE];
        benchmark("string", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < strings.length; i++) {
                    strings[i] = keys[i].toString();
                }
                Arrays.sort(strings);
            }
        }, SIZE);
    }

    @Test
    public void biginteger() throws Exception {

        final BigInteger[] bigIntegers = new BigInteger[SIZE];
        benchmark("biginteger", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < keys.length; i++) {
                    bigIntegers[i] = new BigInteger(1, keys[i].bytes());
                }
                Arrays.sort(bigIntegers);
            }
        }, SIZE);
    }

    @Test
    public void bytes() throws Exception {

        final byte[][] bytesArray = new byte[SIZE][];
        benchmark("bytes", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < keys.length; i++) {
                    bytesArray[i] = keys[i].bytes();
                }
                Arrays.sort(bytesArray, new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] o1, byte[] o2) {
                        for (int i = 0; i < o1.length; i++) {
                            byte b = o1[i];
                            if (o1[i] != o2[i]) return o1[i] > o2[i] ? 1 : -1;
                        }
                        return 0;
                    }
                });
            }
        }, SIZE);
    }

}
