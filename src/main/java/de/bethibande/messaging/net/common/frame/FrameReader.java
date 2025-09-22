package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public interface FrameReader {

    Frame read(final ByteBuf src);

}
