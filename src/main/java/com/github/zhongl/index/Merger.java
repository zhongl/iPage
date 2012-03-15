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

package com.github.zhongl.index;

import com.github.zhongl.codec.Encoder;
import com.google.common.collect.PeekingIterator;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
class Merger {

    protected final File dir;
    protected final Encoder<Index> encoder;

    Merger(File dir, Encoder<Index> encoder) {
        this.dir = dir;
        this.encoder = encoder;
    }

    public IndicesFile merge(PeekingIterator<Index> base, PeekingIterator<Index> delta) throws IOException {
        IndicesFile file = new IndicesFile(dir, encoder);

        while (base.hasNext() && delta.hasNext()) {

            Index a = base.peek();
            Index b = delta.peek();
            Index c;

            int result = a.compareTo(b);

            if (result < 0) c = base.next();       // a <  b, use a
            else if (result > 0) c = delta.next(); // a >  b, use b
            else {                                 // a == b, use b
                c = b;
                delta.next();
                base.next();
            }

            if (c.isRemoved()) continue;           // remove this entry
            file.append(c);
        }

        mergeRestOf(base, file);
        mergeRestOf(delta, file);

        return file;
    }

    private void mergeRestOf(Iterator<Index> iterator, IndicesFile file) throws IOException {
        while (iterator.hasNext()) {
            Index c = iterator.next();
            if (c.isRemoved()) continue;           // remove this entry
            file.append(c);
        }
    }

}