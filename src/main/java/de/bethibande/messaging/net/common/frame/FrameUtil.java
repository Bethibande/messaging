package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class FrameUtil {

    public static void writeKey(final String[] key, final ByteBuf dst) {
        dst.writeShort(key.length);
        for (int i = 0; i < key.length; i++) {
            final String part = key[i];
            final int mark = dst.writerIndex();
            dst.writeShort(0);
            dst.writeCharSequence(part, StandardCharsets.UTF_8);
            dst.setShort(mark, (short) (dst.writerIndex() - mark - 2));
        }
    }

    public static String[] readKey(final ByteBuf src) {
        final short length = src.readShort();
        final String[] key = new String[length];
        for (int i = 0; i < length; i++) {
            final short size = src.readShort();
            key[i] = src.readCharSequence(size, StandardCharsets.UTF_8).toString();
        }
        return key;
    }

}
