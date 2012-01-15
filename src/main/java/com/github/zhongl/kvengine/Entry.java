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

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Entry} is wrapper of bytes, a minimized store unit.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
final class Entry<V> {

    private final Md5Key key;
    private final V value;

    public Entry(Md5Key key, V value) {
        checkNotNull(key, "Key should not be null");
        checkNotNull(value, "Value should not be null");
        this.key = key;
        this.value = value;
    }

    public Md5Key key() {
        return key;
    }

    public V value() {
        return value;
    }

}