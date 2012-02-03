package com.github.zhongl.ex.nio;

import java.io.File;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Transferable {

    long transferFrom(File file, long offset) throws IOException;
}
