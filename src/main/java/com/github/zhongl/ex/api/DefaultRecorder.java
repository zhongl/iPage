package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Journal;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import static com.github.zhongl.ex.nio.ByteBuffers.lengthOf;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
class DefaultRecorder extends Actor implements Recorder, Erasable {

    private final QuanlityOfService quanlityOfService;
    private final Journal journal;
    private final FlowControllor controllor;

    public DefaultRecorder(File dir, QuanlityOfService quanlityOfService, FlowControllor controllor) throws IOException {
        super();
        this.controllor = controllor;
        this.journal = new Journal(dir, new EntryCodec());
        this.quanlityOfService = quanlityOfService;
    }

    @Override
    public boolean append(Md5Key key, byte[] value) {
        checkNotNull(key);
        checkNotNull(value);
        checkArgument(value.length > 0);
        return append(new Entry<Md5Key, byte[]>(key, value));
    }

    @Override
    public boolean remove(Md5Key key) {
        return append(new Entry<Md5Key, byte[]>(key, new byte[0]));
    }

    @Override
    public void eraseTo(final Revision revision) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                journal.eraseTo(revision);
                return null;
            }
        });
    }

    private boolean append(final Entry<Md5Key, byte[]> entry) {
        try {
            controllor.call(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    submit(quanlityOfService.append(journal, entry)).get();
                    return null;
                }
            });

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private class EntryCodec implements Codec {
        @Override
        public ByteBuffer encode(Object instance) {
            Entry<Md5Key, byte[]> entry = (Entry<Md5Key, byte[]>) instance;
            return (ByteBuffer) ByteBuffer.allocate(Md5Key.BYTE_LENGTH + entry.value().length)
                                          .put(entry.key().bytes())
                                          .put(entry.value())
                                          .flip();
        }

        @Override
        public Entry<Md5Key, byte[]> decode(ByteBuffer buffer) {
            byte[] md5 = new byte[Md5Key.BYTE_LENGTH];
            byte[] bytes = new byte[lengthOf(buffer) - Md5Key.BYTE_LENGTH];
            buffer.get(md5);
            buffer.get(bytes);
            return new Entry<Md5Key, byte[]>(new Md5Key(md5), bytes);
        }

        @Override
        public boolean supports(Class<?> type) {
            return Entry.class.equals(type);
        }
    }
}
