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

package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Cursor<T> {
    T get();
}

class Transformer<T> implements Cursor<T> {
    private volatile Cursor<?> delegate;

    public Transformer(Cursor<T> intiCursor) {
        delegate = intiCursor;
    }

    public void transform(Cursor<?> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T get() {
        return (T) delegate.get();
    }

}

class ObjectRef<T> implements Cursor<T> {
    private final T object;
    private final Codec codec;

    ObjectRef(T object, Codec codec) {
        this.object = object;
        this.codec = codec;
    }

    public ByteBuffer encode() {
        return codec.encode(object);
    }

    @Override
    public T get() {
        return object;
    }
}

class Reader<T> implements Cursor<T> {
    private final Page page;
    private final int offset;

    public Reader(Page page, int offset) {
        this.page = page;
        this.offset = offset;
    }

    @Override
    public T get() {
        checkState(page.file().exists());
        ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(page.file());
        buffer.position(offset);
        return page.codec().decode(buffer);
    }
}

interface CursorFactory {

    <T> Cursor<T> reader(int offset);

    <T> ObjectRef<T> objectRef(T object);

    <T> Transformer<T> transformer(Cursor<T> intiCursor);

}

