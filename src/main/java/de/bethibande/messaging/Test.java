package de.bethibande.messaging;

import de.bethibande.memory.Buffer;
import de.bethibande.messaging.concurrent.EventLoopGroup;
import de.bethibande.messaging.nio.Channel;
import de.bethibande.messaging.nio.ServerChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class Test {

    public static volatile long START = 0;
    public static volatile long EXPECTED_BYTES = 0;

    static void main() throws IOException {
        final EventLoopGroup group = new EventLoopGroup(3);

        final InetSocketAddress address = new InetSocketAddress("localhost", 1234);
        final ServerChannel server = new ServerChannel(group, group.next(), ServerSocketChannel.open());
        server.bind(address);

        final Channel client = new Channel(SocketChannel.open(), group.next());
        client.connect(address);

        final byte[] bytes = "Hello World!".getBytes(StandardCharsets.UTF_8);
        final Buffer buffer = Buffer.directNio(bytes.length);
        buffer.write(bytes);

        for (int i = 0; i < 10_000_000; i++) {
            buffer.retain();
            client.write(buffer);
        }
    }

}
