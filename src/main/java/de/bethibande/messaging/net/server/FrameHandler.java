package de.bethibande.messaging.net.server;

import de.bethibande.messaging.net.common.frame.*;
import de.bethibande.messaging.router.MessageRouter;
import de.bethibande.messaging.router.RouteSubscription;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;

import java.util.Map;

public class FrameHandler extends SimpleChannelInboundHandler<Frame> {

    public static final AttributeKey<Map<Long, RouteSubscription>> SUBSCRIPTIONS = AttributeKey.valueOf("subscriptions");

    private final MessageRouter router;

    public FrameHandler(final MessageRouter router) {
        this.router = router;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Frame frame) {
        switch (frame) {
            case MessageFrame msg -> router.post(msg);
            case SubscribeFrame sub -> {
                final RouteSubscription subscription = new RouteSubscription(
                        sub.getSubscriptionId(),
                        ctx.channel(),
                        sub.getPath()
                );

                router.addSubscriber(subscription);

                final Map<Long, RouteSubscription> subscriptions = ctx.channel().attr(SUBSCRIPTIONS).get();
                subscriptions.put(subscription.getId(), subscription);

                final SubscribeAckFrame ack = new SubscribeAckFrame();
                ack.setRequestId(sub.getRequestId());
                ack.setSubscriptionId(subscription.getId());
                ctx.writeAndFlush(ack);
            }
            case UnSubscribeFrame unsub -> {
                final Map<Long, RouteSubscription> subscriptions = ctx.channel().attr(SUBSCRIPTIONS).get();
                final RouteSubscription subscription = subscriptions.remove(unsub.getSubscriptionId());

                if(subscription != null) {
                    router.removeSubscriber(subscription);

                    final UnSubscribeAckFrame ack = new UnSubscribeAckFrame();
                    ack.setRequestId(unsub.getRequestId());
                    ack.setSubscriptionId(subscription.getId());
                    ctx.writeAndFlush(ack);
                }
            }
            default -> throw new IllegalArgumentException("Unknown frame type: " + frame.getClass().getSimpleName());
        }
    }
}
