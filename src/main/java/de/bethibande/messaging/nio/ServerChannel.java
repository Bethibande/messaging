package de.bethibande.messaging.nio;

import de.bethibande.messaging.concurrent.EventLoop;
import de.bethibande.messaging.concurrent.EventLoopGroup;
import de.bethibande.messaging.concurrent.Selectable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class ServerChannel implements Selectable {

    private final EventLoopGroup group;
    private final EventLoop loop;

    private SelectionKey key;

    private final ServerSocketChannel socket;

    public ServerChannel(final EventLoopGroup group,
                         final EventLoop loop,
                         final ServerSocketChannel socket) {
        this.group = group;
        this.loop = loop;
        this.socket = socket;
    }

    public void bind(final InetSocketAddress address) throws IOException {
        this.socket.bind(address);

        this.socket.configureBlocking(false);
        this.key = socket.register(loop.selector(), SelectionKey.OP_ACCEPT);
        this.key.attach(this);
    }

    @Override
    public boolean update() {
        if (key.isAcceptable()) {
            try {
                final SocketChannel socket = this.socket.accept();
                if (socket == null) {
                    return false;
                }

                final Channel channel = new Channel(socket, this.group.next());
                channel.bind(); // Immediately bind to the next event loop
            } catch (IOException e) {
                e.printStackTrace();
                // TODO: Exception handling
            }
            return true;
        }

        return false;
    }

    public void close() throws IOException {
        if (this.key != null) this.key.cancel();
        this.socket.close();
    }
}
