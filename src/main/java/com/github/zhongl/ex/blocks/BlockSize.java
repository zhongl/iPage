package com.github.zhongl.ex.blocks;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public enum BlockSize {
    ONE(1024), TWO(2048), FOUR(4096);

    private final int value;

    BlockSize(int value) {
        this.value = value;
    }

    public int value() {return value;}

}

