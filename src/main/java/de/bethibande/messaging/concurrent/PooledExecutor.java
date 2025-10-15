package de.bethibande.messaging.concurrent;

import de.bethibande.memory.Allocator;
import de.bethibande.memory.impl.PooledAllocator;
import de.bethibande.messaging.net.Selectable;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class PooledExecutor {

    private final TaskQueue queue = new TaskQueue();

    private final long taskBackoffNanos;
    private final long taskSoftBackoffNanos;

    private final AtomicBoolean active = new AtomicBoolean(true);
    private final Selector selector = Selector.open();

    private final PooledAllocator allocator = Allocator.pooled(1 << 16);

    public PooledExecutor(final long taskBackoffNanos, final long taskSoftBackoffNanos) throws IOException {
        this.taskBackoffNanos = taskBackoffNanos;
        this.taskSoftBackoffNanos = taskSoftBackoffNanos;

        scheduleRepeating(this::updateSelector, 1, TimeUnit.MICROSECONDS);
    }

    public Allocator getAllocator() {
        return this.allocator;
    }

    public Selector getSelector() {
        return this.selector;
    }

    public void queue(final Runnable runnable) {
        if (!this.active.get()) return;
        this.queue.queue(runnable);
    }

    public void schedule(final Runnable runnable, final long delay, final TimeUnit unit) {
        if (!this.active.get()) return;
        this.queue.schedule(new Task(System.nanoTime() + unit.toNanos(delay), runnable));
    }

    public void scheduleRepeating(final Runnable runnable, final long delay, final TimeUnit unit) {
        final Runnable task = () -> {
            try {
                runnable.run();
            } finally {
                this.scheduleRepeating(runnable, delay, unit);
            }
        };

        this.schedule(task, delay, unit);
    }

    public void shutdown() {
        this.active.set(false);
    }

    protected void updateSelector() {
        try {
            final int keys = this.selector.selectNow();
            if (keys <= 0) return;

            boolean changed = false;
            int iterations = 0;
            do {
                changed = false;
                // Prevent hot channels from blocking the event loop indefinitely if there are other tasks to complete
                if (!queue.isEmpty()) iterations++;

                for (final SelectionKey selectedKey : this.selector.selectedKeys()) {
                    if (selectedKey.isValid()) {
                        final Selectable selectable = (Selectable) selectedKey.attachment();
                        if (selectable == null) continue;

                        try {
                            final boolean result = selectable.onSelection();
                            changed = changed || result;
                        } catch (final IOException e) {
                            e.printStackTrace(); // TODO: Error handling
                            // TODO: Close connection
                        }
                    }
                }
            } while (changed && iterations < 50);

        } catch (final IOException e) {
            e.printStackTrace(); // TODO: Error handling
        } finally {
            this.selector.selectedKeys().clear();
        }
    }

    protected void softBackoff() {
        final long start = System.nanoTime();
        while (System.nanoTime() - start < this.taskSoftBackoffNanos) {
            Thread.onSpinWait();
        }
    }

    protected void loop() {
        boolean softBackoff = false;
        while (this.active.get() || !this.queue.isEmpty()) {
            final Runnable runnable = this.queue.poll();
            if (runnable != null) {
                softBackoff = false;
                runnable.run();
            } else {
                if (!softBackoff) {
                    softBackoff();
                    softBackoff = true;
                } else {
                    LockSupport.parkNanos(this.taskBackoffNanos);
                }
            }
        }
    }

}
