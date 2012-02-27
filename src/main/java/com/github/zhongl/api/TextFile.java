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

package com.github.zhongl.api;

import com.github.zhongl.index.IndexMerger;
import com.github.zhongl.index.Key;
import com.github.zhongl.line.LineAppender;
import com.github.zhongl.page.Offset;
import com.github.zhongl.page.Page;
import com.github.zhongl.util.Entry;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static com.github.zhongl.api.TextFile.Type.*;
import static com.google.common.io.Files.readLines;

/**
 * Text File Format:
 * <p/>
 * <pre>
 *  TYPE[L, I] NUMBER FILE_NAME \n
 * </pre>
 * <p/>
 * eg:
 * <pre>
 *  L 287463 b8d1b43eae73587ba56baef574709ecb \n
 * </pre>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
class TextFile {

    private final File parent;
    private final Set<File> links;
    private final List<Entry<File, Offset>> lineEntries;
    private final List<Entry<File, Key>> indexEntries;

    public TextFile(File parent, String name) throws IOException {
        this.parent = parent;

        File file = new File(parent, name);
        links = new HashSet<File>();
        links.add(file);

        lineEntries = new LinkedList<Entry<File, Offset>>();
        indexEntries = new LinkedList<Entry<File, Key>>();

        load((file.exists())
                ? readLines(file, Charset.defaultCharset())
                : Collections.<String>emptyList());
    }

    private void load(List<String> strings) {
        for (String string : strings) {
            String[] parts = string.split(" ");
            File pageFile = new File(this.parent, parts[2]);
            links.add(pageFile);

            switch (valueOf(parts[0])) {
                case L:
                    lineEntries.add(new Entry<File, Offset>(pageFile, new Offset(parts[1])));
                    break;
                case I:
                    indexEntries.add(new Entry<File, Key>(pageFile, new Key(parts[1])));
                    break;
                default:
                    throw new IllegalStateException("Unknown type");
            }
        }
    }

    public List<Entry<File, Offset>> lineEntries() {
        return lineEntries;
    }

    public List<Entry<File, Key>> indexEntres() {
        return indexEntries;
    }

    public boolean contains(File file) {

        return links.contains(file);
    }

    public File parent() {
        return parent;
    }

    public File create(IndexMerger indexMerger, LineAppender lineAppender, boolean append) {
        StringBuilder sb = new StringBuilder();

        if (append) {
            for (Entry<File, Offset> entry : lineEntries) {
                appendLineTo(sb, L, entry.value(), entry.key());
            }
        }

        Page line = lineAppender.page();
        if (line.file().length() > 0)
            appendLineTo(sb, L, line.number(), line.file());

        for (Page page : indexMerger.pages())
            appendLineTo(sb, I, page.number(), page.file());

        try {
            File sFile = new File(parent, System.nanoTime() + ".s");
            Files.write(sb.toString(), sFile, Charset.defaultCharset());
            return sFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void appendLineTo(StringBuilder sb, TextFile.Type type, com.github.zhongl.page.Number number, File file) {
        sb.append(type).append(" ").append(number).append(" ").append(file.getName()).append('\n');
    }

    static enum Type {L, I}
}
