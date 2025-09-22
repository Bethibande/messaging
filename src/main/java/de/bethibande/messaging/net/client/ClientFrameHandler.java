package de.bethibande.messaging.net.client;

import de.bethibande.messaging.net.common.frame.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class ClientFrameHandler extends SimpleChannelInboundHandler<Frame> {

    private final MessageClient client;

    public ClientFrameHandler(final MessageClient client) {
        this.client = client;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Frame frame) {
        switch (frame) {
            case SubscribeAckFrame ack -> client.handleRequestFrame(ack);
            case UnSubscribeAckFrame ack -> client.handleRequestFrame(ack);
            case SubMessageFrame msg -> client.handleMessage(msg);
            default -> throw new IllegalArgumentException("Unknown frame type: " + frame);
        }
    }
}
