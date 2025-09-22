package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public interface FrameWriter {

    void write(final Frame frame, final ByteBuf dst);

}
