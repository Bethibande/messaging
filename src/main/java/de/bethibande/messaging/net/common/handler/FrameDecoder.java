package de.bethibande.messaging.net.common.handler;

import de.bethibande.messaging.net.common.frame.Frame;
import de.bethibande.messaging.net.common.frame.Frames;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class FrameDecoder extends SimpleChannelInboundHandler<ByteBuf> {

    private final Frames frames = new Frames();

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) {
        final Frame frame = this.frames.read(msg);
        ctx.fireChannelRead(frame);
    }
}
