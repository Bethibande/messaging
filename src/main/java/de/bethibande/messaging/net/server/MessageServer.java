package de.bethibande.messaging.net.server;

import de.bethibande.messaging.router.MessageRouter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageServer {

    private final MessageRouter router;
    private final InetSocketAddress address;
    private final EventLoopGroup eventLoopGroup;

    private final AtomicBoolean binding = new AtomicBoolean(false);
    private volatile Channel channel;

    public MessageServer(final MessageRouter router,
                         final InetSocketAddress address,
                         final EventLoopGroup eventLoopGroup) {
        this.router = router;
        this.address = address;
        this.eventLoopGroup = eventLoopGroup;
    }

    public boolean isBound() {
        return channel != null;
    }

    public boolean isBinding() {
        return binding.get();
    }

    public ChannelFuture close() {
        if (channel == null) return null;
        return channel.close();
    }

    public ChannelFuture getCloseFuture() {
        if (channel == null) return null;
        return channel.closeFuture();
    }

    public ChannelFuture bind() {
        if (!binding.compareAndSet(false, true)) {
            throw new IllegalStateException("Server is already binding or bound!");
        }

        return new ServerBootstrap()
                .channel(NioServerSocketChannel.class)
                .group(eventLoopGroup)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ServerChannelInitializer(router))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .bind(address)
                .addListener(f -> this.channel = ((ChannelFuture) f).channel());
    }

}
