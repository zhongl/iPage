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
import com.github.zhongl.nio.CommonAccessors;
import com.github.zhongl.util.FileBase;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BlockingKVEngineBenchmark extends FileBase {

    public static final String PROPERTY_PREFIX = "blocking.kvengine.benchmark";
    public static final int TIMES = Integer.getInteger(PROPERTY_PREFIX + ".times", 1000);
    public static final int BUCKET_SIZE = Integer.getInteger(PROPERTY_PREFIX + ".bucket.size", 1024);
    public static final int FLUSH_COUNT = Integer.getInteger(PROPERTY_PREFIX + ".flush.count", 4);
    public static final long FLUSH_ELAPSE = Long.getLong(PROPERTY_PREFIX + ".flush.elpase", 10L);
    public static final int CHUNK_CAPACITY = Integer.getInteger(PROPERTY_PREFIX + ".chunk.capacity", 1024 * 1024 * 64);
    public static final int COLLECT_LENGTH = Integer.getInteger(PROPERTY_PREFIX + ".collect.length", 1024 * 1024 * 64);
    public static final boolean GROUP_COMMIT = Boolean.getBoolean(PROPERTY_PREFIX + ".group.commit");
    public static final boolean AUTO_GC = Boolean.getBoolean(PROPERTY_PREFIX + ".auto.gc");

    private BlockingKVEngine engine;

    @After
    public void tearDown() throws Exception {
        if (engine != null) engine.shutdown();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        printOptions();

        super.setUp();
        dir = testDir("benchmark");
        engine = new BlockingKVEngine(
                KVEngine.baseOn(dir)
                        .initialBucketSize(BUCKET_SIZE)
                        .flushCount(FLUSH_COUNT)
                        .flushElapseMilliseconds(FLUSH_ELAPSE)
                        .maximizeChunkCapacity(CHUNK_CAPACITY)
                        .valueAccessor(CommonAccessors.BYTES)
                        .minimzieCollectLength(COLLECT_LENGTH)
                        .groupCommit(GROUP_COMMIT)
                        .startAutoGarbageCollectOnStartup(AUTO_GC)
                        .build());
        engine.startup();
    }

    private void printOptions() throws IllegalAccessException {
        Field[] fields = BlockingKVEngineBenchmark.class.getFields();
        for (Field field : fields) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers)) {
                System.out.println(field.getName() + "=" + field.get(null));
            }
        }
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

        Thread.sleep(50L);
    }

    abstract static class OperationFactory implements CallableFactory {
        protected final BlockingKVEngine engine;
        private final AtomicInteger count = new AtomicInteger();

        public OperationFactory(BlockingKVEngine engine) {this.engine = engine;}

        protected byte[] generateValue() {
            return Bytes.concat(Ints.toByteArray(count.getAndIncrement()), new byte[1020]);
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
