package de.bethibande.messaging;

import de.bethibande.memory.Buffer;
import de.bethibande.messaging.concurrent.ExecutorPool;
import de.bethibande.messaging.concurrent.PooledExecutor;
import de.bethibande.messaging.net.Connection;
import de.bethibande.messaging.net.MessagingServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class Test {

    public static volatile long START = 0;
    public static volatile long EXPECTED_BYTES = 0;

    static void main() throws IOException  {
        final ExecutorPool pool = new ExecutorPool(32);

        final PooledExecutor executor = pool.getExecutor();
        executor.scheduleRepeating(() -> System.out.println("Interval"), 1, TimeUnit.SECONDS);
        executor.schedule(() -> System.out.println("Later"), 5, TimeUnit.SECONDS);
        executor.queue(() -> System.out.println("Hello World!"));
        //executor.schedule(pool::shutdown, 5, TimeUnit.SECONDS);

        final InetSocketAddress address = new InetSocketAddress(5555);
        final MessagingServer server = new MessagingServer(ServerSocketChannel.open(), pool, address);
        server.bind();

        final Connection connection = new Connection(SocketChannel.open(), pool.getExecutor());
        connection.connect(address);

        final Buffer buffer = Buffer.direct(1024);
        buffer.write("Hello World!".getBytes());
        buffer.retain();
        connection.write(buffer);

        final Buffer buffer2 = Buffer.direct(1024);
        buffer2.write("Hello World!".getBytes());
        connection.write(buffer2);

        /*for (int i = 0; i < 10_000; i++) {
            buffer.retain();
            connection.write(buffer);
        }*/


        final long start = System.currentTimeMillis();
        final int iterations = 1_000_000;
        EXPECTED_BYTES = iterations * buffer.readable();
        START = start;
        for (int i = 0; i < iterations; i++) {
            connection.write(buffer);
            buffer.retain();
        }
        final long time = System.currentTimeMillis() - start;
        System.out.println("Took " + time + "ms");

        final double nsOp = (double) time / iterations / 1_000_000;
        final double opsPerSec = 1_000_000_000 / nsOp;

        System.out.println("Ops/s: " + opsPerSec);

        pool.shutdown();
    }

}
