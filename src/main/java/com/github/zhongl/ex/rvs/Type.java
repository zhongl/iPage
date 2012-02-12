package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.ex.util.Tuple;

import java.io.File;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
enum Type {
    L { // Line

        @Override
        public void apply(String number, String filename, File parentFile, List<Tuple> linePageTuples, List<Tuple> indexPageTuples) {
            linePageTuples.add(new Tuple(new Offset(number), new File(parentFile, filename)));
        }
    }, I { // Index

        @Override
        public void apply(String number, String filename, File parentFile, List<Tuple> linePageTuples, List<Tuple> indexPageTuples) {
            indexPageTuples.add(new Tuple(new Key(number), new File(parentFile, filename)));
        }
    };

    public abstract void apply(String number, String filename, File parentFile, List<Tuple> linePageTuples, List<Tuple> indexPageTuples);
}
