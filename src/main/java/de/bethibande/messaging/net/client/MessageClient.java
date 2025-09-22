package de.bethibande.messaging.net.client;

import de.bethibande.messaging.net.common.AbstractRequest;
import de.bethibande.messaging.net.common.frame.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class MessageClient {

    private final AtomicLong requestIdCounter = new AtomicLong();
    private final AtomicLong subscriptionIdCounter = new AtomicLong();

    private final InetSocketAddress address;
    private final EventLoopGroup eventLoopGroup;

    private final Map<Long, AbstractRequest<?>> pendingRequests = new HashMap<>();
    private final Map<Long, ClientSubscription> subscriptions = new HashMap<>();

    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private volatile Channel channel;

    public MessageClient(final InetSocketAddress address, final EventLoopGroup eventLoopGroup) {
        this.address = address;
        this.eventLoopGroup = eventLoopGroup;
    }

    public ChannelFuture close() {
        if (channel == null) return null;
        return channel.close();
    }

    protected void handleMessage(final SubMessageFrame msg) {
        final ClientSubscription sub = subscriptions.get(msg.getSubscriptionId());
        if (sub != null) {
            sub.receive(msg);
        }
    }

    protected void registerSubscription(final ClientSubscription subscription) {
        this.subscriptions.put(subscription.getId(), subscription);
    }

    protected void unregisterSubscription(final ClientSubscription subscription) {
        this.subscriptions.remove(subscription.getId());
    }

    protected <T extends RequestFrame & Frame> void handleRequestFrame(final T frame) {
        final AbstractRequest<?> request = findRequest(frame.getRequestId());
        if (request == null) {
            throw new IllegalStateException("Unknown request id: " + frame.getRequestId());
        }
        request.receive(frame);
    }

    protected <T extends AbstractRequest<?>> T createRequest(final Function<Long, T> factory) {
        final long id = requestIdCounter.incrementAndGet();
        final T request = factory.apply(id);
        registerRequest(request);

        return request;
    }

    private void registerRequest(final AbstractRequest<?> request) {
        pendingRequests.put(request.getId(), request);
        request.getFuture().addListener(f -> this.pendingRequests.remove(request.getId()));
    }

    protected AbstractRequest<?> findRequest(final long id) {
        return pendingRequests.get(id);
    }

    public Promise<ClientSubscription> subscribe(final String... key) {
        // TODO: Timeout config
        final ClientSubscribeRequest request = createRequest(id -> new ClientSubscribeRequest(
                id,
                channel,
                10_000,
                subscriptionIdCounter.incrementAndGet(),
                key,
                this
        ));

        final SubscribeFrame frame = new SubscribeFrame();
        frame.setRequestId(request.getId());
        frame.setSubscriptionId(request.getSubscriptionId());
        frame.setPath(key);
        writeAndFlush(frame);

        return request.getFuture();
    }

    public ChannelFuture connect() {
        if (!this.connecting.compareAndSet(false, true)) {
            throw new IllegalStateException("Client is already connecting or connected!");
        }

        return new Bootstrap()
                .channel(NioSocketChannel.class)
                .group(eventLoopGroup)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ClientChannelInitializer(this))
                .connect(address)
                .addListener(f -> this.channel = ((ChannelFuture) f).channel());
    }

    protected ChannelFuture writeAndFlush(final Frame frame) {
        return this.channel.writeAndFlush(frame);
    }

    public ChannelFuture write(final String[] key, final Consumer<ByteBuf> writer) {
        final ByteBuf buf = this.channel.alloc().buffer();
        try {
            writer.accept(buf);
        } catch (final Throwable th) {
            buf.release();
            throw th;
        }

        return this.channel.writeAndFlush(new MessageFrame(key, buf)).addListener(f -> buf.release());
    }

}
