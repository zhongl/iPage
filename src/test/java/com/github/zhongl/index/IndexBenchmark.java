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

import com.github.zhongl.benchmarker.*;
import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.util.FileTestContext;
import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexBenchmark extends FileTestContext {

    private Index index;

    @After
    public void tearDown() throws Exception {
        if (index != null) index.close();
    }

    @Test
    public void benchmark() throws Exception {
        dir = testDir("benchmark");

        index = new Index(dir, 100);

        CallableFactory addFactory = new AddFactory(index);
        CallableFactory replaceFactory = new ReplaceFactory(index);
        CallableFactory getFactory = new GetFactory(index);
        CallableFactory removeFactory = new RemoveFactory(index);


        CallableFactory concatCallableFactory = new ConcatCallableFactory(
                new FixInstanceSizeFactory(100, addFactory),
                new FixInstanceSizeFactory(100, replaceFactory),
                new FixInstanceSizeFactory(100, getFactory),
                new FixInstanceSizeFactory(100, removeFactory)
        );

        Collection<Statistics> statisticses =
                new Benchmarker(concatCallableFactory, 1, 400).benchmark(); // setup concurrent 1, because index is not thread-safe
        for (Statistics statisticse : statisticses) {
            System.out.println(statisticse);
        }
    }

    abstract static class OperationFactory implements CallableFactory {
        protected final Index index;
        private int count;

        public OperationFactory(Index index) {this.index = index;}

        protected byte[] genKey() {
            return Ints.toByteArray(count++);
        }
    }

    private static class AddFactory extends OperationFactory {

        public AddFactory(Index index) {
            super(index);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return index.put(Md5Key.generate(genKey()), genOffset());
                }
            };
        }

        protected Cursor genOffset() {
            return new Cursor(7L);
        }

    }

    private static class ReplaceFactory extends AddFactory {

        public ReplaceFactory(Index index) {
            super(index);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return index.put(Md5Key.generate(genKey()), genOffset());
                }
            };
        }


    }

    private static class GetFactory extends OperationFactory {

        public GetFactory(Index index) {
            super(index);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return index.get(Md5Key.generate(genKey()));
                }
            };
        }

    }

    private static class RemoveFactory extends OperationFactory {

        public RemoveFactory(Index index) {
            super(index);
        }

        @Override
        public Callable<?> create() {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return index.remove(Md5Key.generate(genKey()));
                }
            };
        }

    }
}
