package com.github.zhongl.util;

import java.io.File;
import java.util.Comparator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileNumberNameComparator implements Comparator<File> {

    @Override
    public int compare(File o1, File o2) {
        return (int) (Long.parseLong(o1.getName()) - Long.parseLong(o2.getName()));
    }
}
