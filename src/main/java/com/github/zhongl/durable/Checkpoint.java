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

package com.github.zhongl.durable;

import com.github.zhongl.journal.Event;
import com.github.zhongl.page.Page;
import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.FilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class Checkpoint {

    private final File dir;
    private final int groupApplyLength;

    private volatile long lastNumber;
    private volatile Cursor lastCursor;

    public Checkpoint(File dir, int groupApplyLength) throws IOException {
        this.dir = dir;
        this.groupApplyLength = groupApplyLength;
        tryLoadLastCheckpoint();
    }

    public boolean isApplied(Page<Event> page) {
        return page.number() <= lastNumber;
    }

    public boolean trySaveBy(Cursor cursor, long number) throws IOException {
        if (cursor.distanceTo(lastCursor) < groupApplyLength) return false;
        new File(dir, number + "." + cursor).createNewFile();
        new File(dir, lastNumber + "." + lastCursor).delete();
        lastNumber = number;
        lastCursor = cursor;
        return true;
    }

    public Cursor lastCursor() {
        return lastCursor;
    }

    private void tryLoadLastCheckpoint() throws IOException {
        if (!dir.exists()) dir.mkdirs();

        ArrayList<String> list = new FilesLoader<String>(
                dir,
                new FilterAndComparator() {
                    @Override
                    public int compare(File o1, File o2) {
                        return (int) (Double.parseDouble(o1.getName()) - Double.parseDouble(o2.getName()));
                    }

                    @Override
                    public boolean accept(File dir, String name) {
                        return name.matches("\\d+\\.\\d+");
                    }
                },
                new Transformer<String>() {
                    @Override
                    public String transform(File file, boolean last) throws IOException {
                        if (last) return file.getName();
                        file.delete();
                        return null;
                    }
                }
        ).loadTo(new ArrayList<String>());

        if (list.isEmpty()) {
            lastNumber = 0L;
            lastCursor = Cursor.head();
        } else {
            String[] split = list.get(0).split("\\.");
            lastNumber = Long.parseLong(split[0]);
            lastCursor = Cursor.valueOf(split[1]);
        }
    }
}
