package de.bethibande.messaging.net.server;

import de.bethibande.messaging.net.common.handler.FrameDecoder;
import de.bethibande.messaging.net.common.handler.FrameEncoder;
import de.bethibande.messaging.router.MessageRouter;
import de.bethibande.messaging.router.RouteSubscription;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.util.HashMap;
import java.util.Map;

public class ServerChannelInitializer extends ChannelInitializer<NioSocketChannel> {

    private final MessageRouter router;

    public ServerChannelInitializer(final MessageRouter router) {
        this.router = router;
    }

    @Override
    protected void initChannel(final NioSocketChannel ch) {
        final ChannelPipeline pipe = ch.pipeline();

        pipe.addLast(new FrameEncoder());
        pipe.addLast(new LengthFieldBasedFrameDecoder(Short.MAX_VALUE, 0, 2, 0, 2));
        pipe.addLast(new FrameDecoder());
        pipe.addLast(new FrameHandler(router));

        ch.attr(FrameHandler.SUBSCRIPTIONS).set(new HashMap<>());

        ch.closeFuture().addListener(_ -> {
            final Map<Long, RouteSubscription> subscriptions = ch.attr(FrameHandler.SUBSCRIPTIONS).get();
            for (final RouteSubscription subscription : subscriptions.values()) {
                subscription.remove();
            }
        });
    }
}
