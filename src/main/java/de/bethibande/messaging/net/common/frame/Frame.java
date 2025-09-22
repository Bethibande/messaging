package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public sealed interface Frame permits MessageFrame, SubMessageFrame, SubscribeFrame, SubscribeAckFrame, UnSubscribeFrame, UnSubscribeAckFrame {

    void read(final ByteBuf src, final FrameReader reader);

    void write(final ByteBuf dst, final FrameWriter writer);

}
