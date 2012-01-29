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

package com.github.zhongl.ex.nio;

import java.io.File;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappingOptions {
    private final File file;
    private final FileChannel.MapMode mode;
    private final long position;
    private final int size;

    public MappingOptions(File file, FileChannel.MapMode mode, long position, int size) {
        checkNotNull(file);
        checkNotNull(mode);
        mkdirIfNotExist(file);
        this.file = file;
        this.mode = mode;
        this.position = position;
        this.size = size;
    }

    private void mkdirIfNotExist(File file) {
        File parentFile = file.getParentFile();
        if (!parentFile.exists()) checkState(parentFile.mkdirs());
    }

    public File file() {
        return file;
    }

    public FileChannel.MapMode mode() {
        return mode;
    }

    public long position() {
        return position;
    }

    public int size() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingOptions that = (MappingOptions) o;

        return position == that.position
                && size == that.size
                && file.equals(that.file)
                && mode.equals(that.mode);

    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + mode.hashCode();
        result = 31 * result + (int) (position ^ (position >>> 32));
        result = 31 * result + size;
        return result;
    }

    @Override
    public String toString() {
        return "MappingOptions{" +
                "file=" + file +
                ", mode=" + mode +
                ", position=" + position +
                ", size=" + size +
                '}';
    }
}
