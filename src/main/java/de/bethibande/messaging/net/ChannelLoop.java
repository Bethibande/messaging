package de.bethibande.messaging.net;

import de.bethibande.messaging.concurrent.PooledExecutor;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class ChannelLoop {

    private final SocketChannel channel;
    private final PooledExecutor executor;
    private final SelectionKey key;

    public ChannelLoop(final SocketChannel channel, final PooledExecutor executor) throws IOException {
        this.channel = channel;
        this.executor = executor;

        this.channel.configureBlocking(false);
        this.key = this.channel.register(this.executor.getSelector(), SelectionKey.OP_READ);
    }

    protected void loop() {
        if (!key.isReadable()) {
            executor.schedule(this::loop, 1, TimeUnit.MICROSECONDS);
            return;
        }

        channel.read(null, 1, 1)
    }

}
