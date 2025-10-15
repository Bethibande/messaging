package de.bethibande.messaging.net;

import de.bethibande.messaging.concurrent.ExecutorPool;
import de.bethibande.messaging.concurrent.PooledExecutor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class MessagingServer implements Selectable {

    private final ServerSocketChannel channel;
    private SelectionKey key;
    private final ExecutorPool pool;
    private final PooledExecutor executor;
    private final InetSocketAddress address;

    public MessagingServer(final ServerSocketChannel channel,
                           final ExecutorPool pool,
                           final InetSocketAddress address) {
        this.channel = channel;
        this.pool = pool;
        this.executor = pool.getExecutor();
        this.address = address;
    }

    public void bind() throws IOException {
        this.channel.bind(this.address);
        channel.configureBlocking(false);

        this.key = channel.register(executor.getSelector(), SelectionKey.OP_ACCEPT);
        this.key.attach(this);
    }

    @Override
    public boolean onSelection() throws IOException {
        if (!channel.isOpen() || !key.isAcceptable()) return false;

        final SocketChannel connectionSocket = channel.accept();
        if (connectionSocket == null) return false;

        final Connection connection = new Connection(connectionSocket, this.pool.getExecutor());
        connection.register();

        return true;
    }
}
