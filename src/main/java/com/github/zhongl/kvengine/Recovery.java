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

import com.github.zhongl.index.Index;
import com.github.zhongl.index.Slot;
import com.github.zhongl.integrity.Validator;
import com.github.zhongl.ipage.IPage;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Recovery<V> implements Runnable {

    private final Index index;
    private final IPage<Entry<V>> ipage;

    public Recovery(Index index, IPage<Entry<V>> ipage) {
        this.index = index;
        this.ipage = ipage;
    }

    @Override
    public void run() {
        try {
            index.validateOrRecoverBy(new Validator<Slot, IOException>() {
                @Override
                public boolean validate(Slot slot) throws IOException {
//                    Entry<V> entry = ipage.get(slot.cursor());
//                    if (entry == null) return false;
//                    return entry.key().equals(slot.key());
                    return false;
                }
            });
            ipage.validateOrRecoverBy(new Validator<Entry<V>, IOException>() {
                @Override
                public boolean validate(Entry<V> entry) throws IOException {
                    return index.get(entry.key()) != null;
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException("Can't run recovery, because:", e);
        }
    }

}
