package de.bethibande.messaging.net.client;

import de.bethibande.messaging.net.common.handler.FrameDecoder;
import de.bethibande.messaging.net.common.handler.FrameEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class ClientChannelInitializer extends ChannelInitializer<NioSocketChannel> {

    private final MessageClient client;

    public ClientChannelInitializer(final MessageClient client) {
        this.client = client;
    }

    @Override
    protected void initChannel(final NioSocketChannel ch) {
        final ChannelPipeline pipe = ch.pipeline();

        pipe.addLast(new FrameEncoder());
        pipe.addLast(new LengthFieldBasedFrameDecoder(Short.MAX_VALUE, 0, 2, 0, 2));
        pipe.addLast(new FrameDecoder());
        pipe.addLast(new ClientFrameHandler(this.client));
    }
}
