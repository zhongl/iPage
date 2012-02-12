package com.github.zhongl.ipage;

import com.github.zhongl.util.RAFileChannel;
import com.google.common.io.Closeables;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class LineAppender {

    private final InnerPage page;

    protected LineAppender(File dir, long position) {
        Offset offset = new Offset(position);
        page = new InnerPage(new File(dir, offset.toString()), offset);
    }

    public int append(ByteBuffer buffer) {
        return page.append(buffer);
    }

    public void force() {
        page.force();
    }

    private static class InnerPage extends Page {

        private final RAFileChannel channel;

        protected InnerPage(File file, Offset number) {
            super(file, number);
            try {
                channel = new RAFileChannel(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public int append(ByteBuffer buffer) {
            try {
                return channel.write(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void force() {
            try {
                channel.force(false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                Closeables.closeQuietly(channel);
            }
        }
    }
}
