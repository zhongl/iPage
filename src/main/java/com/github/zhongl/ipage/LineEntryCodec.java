package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class LineEntryCodec<V> {
    public static final int KEY_AND_LEN = Key.BYTE_LENGTH + 4;
    private final Codec<V> vCodec;

    LineEntryCodec(Codec<V> vCodec) {
        this.vCodec = vCodec;
    }

    public ByteBuffer encode(Entry<Key, V> entry) {
        ByteBuffer buffer = vCodec.encode(entry.value());
        int length = buffer.remaining();
        ByteBuffer aggregated = ByteBuffer.allocate(KEY_AND_LEN + length);
        return (ByteBuffer) aggregated.put(entry.key().bytes())
                                      .putInt(length)
                                      .put(buffer)
                                      .flip();

    }

    public LazyDecoder<V> lazyDecoder(final ByteBuffer buffer) {
        int from = buffer.position();
        buffer.position(from + Key.BYTE_LENGTH);
        int length = buffer.getInt();
        int to = buffer.position() + length;
        buffer.position(to);
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.limit(to).position(from);
        final ByteBuffer origin = duplicate.slice();
        return new LazyDecoder() {

            @Override
            public Key key() {
                byte[] bytes = new byte[Key.BYTE_LENGTH];
                origin.position(0);
                origin.get(bytes);
                return new Key(bytes);
            }

            public V value() {
                origin.position(KEY_AND_LEN);
                return vCodec.decode(origin);
            }

            @Override
            public ByteBuffer origin() {
                return (ByteBuffer) origin.position(0);
            }
        };
    }

    interface LazyDecoder<V> {
        Key key();

        V value();

        ByteBuffer origin();
    }
}
