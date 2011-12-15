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

package com.github.zhongl.kvengine;

import com.github.zhongl.benchmarker.*;
import com.github.zhongl.buffer.CommonAccessors;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.util.FileBase;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BlockingKVEngineBenchmark extends FileBase {

    public static final int TIMES = Integer.getInteger("blocking.kvengine.benchmark.times", 1000);
    private BlockingKVEngine<byte[]> engine;

    @After
    public void tearDown() throws Exception {
        engine.shutdown();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dir = testDir("benchmark");
        engine = new BlockingKVEngine<byte[]>(
                KVEngine.<byte[]>baseOn(dir)
                        .initialBucketSize(8192)
                        .flushCount(4)
                        .flushElapseMilliseconds(10L)
                        .maximizeChunkCapacity(1024 * 1024 * 128)
                        .valueAccessor(CommonAccessors.BYTES)
                        .groupCommit(true)
                        .build());
        engine.startup();
    }

    @Test
    public void benchmark() throws Exception {

        CallableFactory addFactory = new PutFactory(engine);
        CallableFactory getFactory = new GetFactory(engine);
        CallableFactory removeFactory = new RemoveFactory(engine);

        CallableFactory concatCallableFactory = new ConcatCallableFactory(
                new FixInstanceSizeFactory(TIMES, addFactory),
                new FixInstanceSizeFactory(TIMES, getFactory),
                new FixInstanceSizeFactory(TIMES, removeFactory)
        );

        Collection<Statistics> statisticses =
                new Benchmarker(concatCallableFactory, 8, TIMES * 3).benchmark();
        for (Statistics statisticse : statisticses) {
            System.out.println(statisticse);
        }

        engine.garbageCollect();
    }

    abstract static class OperationFactory implements CallableFactory {
        protected final BlockingKVEngine engine;
        private int count;

        public OperationFactory(BlockingKVEngine engine) {this.engine = engine;}

        protected byte[] generateValue() {
            return Bytes.concat(Ints.toByteArray(count++), new byte[1020]);
        }
    }

    private static class PutFactory extends OperationFactory {

        public PutFactory(BlockingKVEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    byte[] value = generateValue();
                    return engine.put(Md5Key.generate(value), value);
                }
            };
        }

    }

    private static class GetFactory extends OperationFactory {

        public GetFactory(BlockingKVEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return engine.get(Md5Key.generate(generateValue()));
                }
            };
        }

    }

    private static class RemoveFactory extends OperationFactory {

        public RemoveFactory(BlockingKVEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return engine.remove(Md5Key.generate(generateValue()));
                }
            };
        }

    }
}
