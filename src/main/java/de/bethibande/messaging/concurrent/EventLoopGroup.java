package de.bethibande.messaging.concurrent;

import de.bethibande.memory.Allocator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EventLoopGroup {

    protected static final AtomicInteger ID_COUNTER = new AtomicInteger(0);

    protected final int id = ID_COUNTER.getAndIncrement();

    protected final AtomicInteger loopIdCounter = new AtomicInteger(0);

    protected final EventLoop[] loops;
    protected final AtomicLong counter = new AtomicLong(0);

    public EventLoopGroup(final int threads) throws IOException {
        this.loops = new EventLoop[threads];
        for (int i = 0; i < threads; i++) {
            final EventLoop loop = this.newLoop();
            this.loops[i] = loop;

            runLoop(loop);
        }
    }

    protected void runLoop(final EventLoop loop) {
        Thread.ofPlatform()
                .name("group-%d-loop-%d".formatted(id, loopIdCounter.getAndIncrement()))
                .start(loop::loop);
    }

    protected EventLoop newLoop() throws IOException {
        final Allocator allocator = Allocator.pooled(1 << 16);
        return new EventLoop(allocator, this);
    }

    public EventLoop next() {
        return this.loops[(int) (this.counter.getAndIncrement() % this.loops.length)];
    }

    public void close() {
        for (final EventLoop loop : loops) {
            loop.close();
        }
    }
}
