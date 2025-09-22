package de.bethibande.messaging;

import de.bethibande.messaging.net.client.ClientSubscription;
import de.bethibande.messaging.net.client.MessageClient;
import de.bethibande.messaging.net.server.MessageServer;
import de.bethibande.messaging.router.MessageRouter;
import de.bethibande.messaging.router.RouterNode;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class Test {

    public static void main() throws InterruptedException, ExecutionException {
        final ThreadLocal<Queue<RouterNode>> queues = ThreadLocal.withInitial(ArrayDeque::new);
        final MessageRouter router = new MessageRouter(queues::get);
        final EventLoopGroup eventLoopGroup = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        final InetSocketAddress address = new InetSocketAddress("localhost", 12345);

        final MessageServer server = new MessageServer(router, address, eventLoopGroup);
        server.bind().sync();

        final MessageClient client = new MessageClient(address, eventLoopGroup);
        client.connect().sync();

        final ClientSubscription sub = client.subscribe("test").get();

        for (int i = 0; i < 50_000; i++) {
            client.write(new String[]{"test"}, buf -> buf.writeByte(1)).sync();
        }

        final int iterations = 1_000_000;
        final int threads = 4;

        final AtomicInteger received = new AtomicInteger();
        sub.addListener(_ -> received.incrementAndGet());

        final long start = System.currentTimeMillis();
        for (int t = 0; t < threads; t++) {
            Thread.ofPlatform().start(() -> {
                for (int i = 0; i < iterations / threads; i++) {
                    client.write(new String[]{"test"}, buf -> buf.writeByte(1));
                }
            });
        }

        while(received.get() < iterations) {
            LockSupport.parkNanos(10);
        }

        final long time = System.currentTimeMillis() - start;
        final double ops = iterations / (time / 1000.0);
        final double microsPerOp = ((double) iterations / time) * 1000;
        System.out.println("Took " + time + "ms for " + iterations + " operations (" + ops + " ops/s)");
        System.out.println(microsPerOp + " µs/op");

        sub.remove().sync();

        client.close().sync();
        server.close().sync();
        eventLoopGroup.shutdownGracefully();
    }

}
