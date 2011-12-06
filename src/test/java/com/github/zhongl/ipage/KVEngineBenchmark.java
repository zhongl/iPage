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

package com.github.zhongl.ipage;

import com.github.zhongl.benchmarker.*;
import com.github.zhongl.util.FileBase;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineBenchmark extends FileBase {

    private KVEngine engine;

    @After
    public void tearDown() throws Exception {
        engine.shutdown();
        engine.awaitForShutdown(Long.MAX_VALUE);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dir = testDir("benchmark");
        engine = KVEngine.baseOn(dir)
                .initialBucketSize(100)
                .flushByCount(5)
                .flushByElapseMilliseconds(10L)
                .chunkCapacity(1024 * 1024 * 32)
                .groupCommit(true)
                .build();
        engine.startup();
    }

    @Test
    public void benchmark() throws Exception {

        CallableFactory addFactory = new PutFactory(engine);
        CallableFactory getFactory = new GetFactory(engine);
        CallableFactory removeFactory = new RemoveFactory(engine);

        CallableFactory concatCallableFactory = new ConcatCallableFactory(
                new FixInstanceSizeFactory(1000, addFactory),
                new FixInstanceSizeFactory(1000, getFactory),
                new FixInstanceSizeFactory(1000, removeFactory)
        );

        Collection<Statistics> statisticses =
                new Benchmarker(concatCallableFactory, 8, 3000).benchmark(); // setup concurrent 1, because engine is not thread-safe
        for (Statistics statisticse : statisticses) {
            System.out.println(statisticse);
        }
    }

    abstract static class OperationFactory implements CallableFactory {
        protected final KVEngine engine;
        private int count;

        public OperationFactory(KVEngine engine) {this.engine = engine;}

        protected Record genRecord() {
            return new Record(Bytes.concat(Ints.toByteArray(count++), new byte[1020]));
        }
    }

    private static class PutFactory extends OperationFactory {

        public PutFactory(KVEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Record record = genRecord();
                    return engine.put(Md5Key.valueOf(record), record);
                }
            };
        }

    }

    private static class GetFactory extends OperationFactory {

        public GetFactory(KVEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return engine.get(Md5Key.valueOf(genRecord()));
                }
            };
        }

    }

    private static class RemoveFactory extends OperationFactory {

        public RemoveFactory(KVEngine engine) {
            super(engine);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return engine.remove(Md5Key.valueOf(genRecord()));
                }
            };
        }

    }
}
