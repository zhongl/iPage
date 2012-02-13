package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Md5;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

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

    private final List<Entry<File, Offset>> lineEntries = new LinkedList<Entry<File, Offset>>();
    private final List<Entry<File, Key>> indexEntries = new LinkedList<Entry<File, Key>>();
    private final Set<File> pageFiles = new HashSet<File>();

    public TextFile(File file) throws IOException {
        List<String> strings = (file == null) ?
                Collections.<String>emptyList() :
                Files.readLines(file, Charset.defaultCharset());

        pageFiles.add(file);

        for (String string : strings) {
            String[] parts = string.split(" ");
            File pageFile = new File(file.getParentFile(), parts[2]);
            pageFiles.add(pageFile);

            switch (Type.valueOf(parts[0])) {
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

        return pageFiles.contains(file);
    }

    public File create(IndexMerger indexMerger, LineAppender lineAppender, File tmp, boolean append) {
        StringBuilder sb = new StringBuilder();

        if (append) {
            for (Entry<File, Offset> entry : lineEntries) {
                appendLineTo(sb, TextFile.Type.L, entry.value(), entry.key());
            }
        }

        Page line = lineAppender.page();
        if (line.file().length() > 0)
            appendLineTo(sb, TextFile.Type.L, line.number(), Md5.renameToMd5(line.file()));

        for (Page page : indexMerger.pages) {
            appendLineTo(sb, TextFile.Type.I, page.number(), Md5.renameToMd5(page.file()));
        }

        try {
            File sFile = new File(tmp, "snapshot");
            Files.write(sb.toString(), sFile, Charset.defaultCharset());
            return Md5.renameToMd5(sFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void appendLineTo(StringBuilder sb, TextFile.Type type, Number number, File file) {
        sb.append(type).append(" ").append(number).append(" ").append(file.getName()).append('\n');
    }

    static enum Type {L, I}
}
