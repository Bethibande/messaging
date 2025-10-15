package de.bethibande.messaging.concurrent;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorPool {

    private final AtomicInteger offset = new AtomicInteger(0);
    private final List<PooledExecutor> executors = new ArrayList<>();

    public ExecutorPool(final int executors) throws IOException {
        for (int i = 0; i < executors; i++) {
            final PooledExecutor executor = new PooledExecutor(950, 50);
            Thread.ofPlatform().start(executor::loop);

            executor.schedule(() -> {
                try {
                    final Selector selector = executor.getSelector();
                    if (selector != null && selector.isOpen()) selector.selectNow();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, 1, TimeUnit.MICROSECONDS);

            this.executors.add(executor);
        }
    }

    public PooledExecutor getExecutor() {
        return this.executors.get(this.offset.getAndIncrement() % this.executors.size());
    }

    public void shutdown() {
        this.executors.forEach(PooledExecutor::shutdown);
    }

}
