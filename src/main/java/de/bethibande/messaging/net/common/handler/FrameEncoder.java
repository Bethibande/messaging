package de.bethibande.messaging.net.common.handler;

import de.bethibande.messaging.net.common.frame.Frame;
import de.bethibande.messaging.net.common.frame.Frames;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class FrameEncoder extends MessageToByteEncoder<Frame> {

    private final Frames frames = new Frames();

    @Override
    protected void encode(final ChannelHandlerContext ctx, final Frame msg, final ByteBuf out) {
        frames.write(msg, out);
    }
}
