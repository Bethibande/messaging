package de.bethibande.messaging.concurrent;

import de.bethibande.memory.Allocator;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class EventLoop {

    public static final int TIMEOUT_SHORT = 50;
    public static final int TIMEOUT_LONG = 1_000;

    public static final int ITERATION_LIMIT = 50;

    protected final Selector selector = Selector.open();
    protected final Allocator allocator;
    protected final EventLoopGroup group;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public EventLoop(final Allocator allocator, final EventLoopGroup group) throws IOException {
        this.allocator = allocator;
        this.group = group;
    }

    protected void loop() {
        int tries = 0;
        while (!closed.get()) {
            try {
                final int keys = selector.selectNow();
                if (keys <= 0) {
                    tries++;

                    if (tries > 10) {
                        LockSupport.parkNanos(TIMEOUT_LONG);
                        continue;
                    } else {
                        final long start = System.nanoTime();
                        while (System.nanoTime() - TIMEOUT_SHORT < start) {
                            Thread.onSpinWait();
                        }
                        continue;
                    }
                }

                tries = 0; // Reset tries if there are pending operations

                processKeys(); // Process pending operations

                selector.selectedKeys().clear(); // Clear for next iteration
            } catch (final IOException e) {
                // TODO: Exception handling
                e.printStackTrace();
            }
        }
    }

    protected void processKeys() {
        boolean hasUpdates = true;
        // We must ensure we don't get stuck in this loop forever.
        // Otherwise, the selector is never updated and some channels will never be processed.
        int iteration = 0;

        final SelectionKey[] keys = selector.selectedKeys().toArray(SelectionKey[]::new);
        final Selectable[] selectables = new Selectable[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final SelectionKey key = keys[i];
            selectables[i] = (Selectable) key.attachment();
        }

        while (hasUpdates && iteration < ITERATION_LIMIT) {
            hasUpdates = false;

            for (int i = 0; i < keys.length; i++) {
                final Selectable selectable = selectables[i];
                if (selectable == null) continue;

                hasUpdates |= selectable.update();
            }

            iteration++;
        }
    }

    public Allocator allocator() {
        return this.allocator;
    }

    public Selector selector() {
        return this.selector;
    }

    public void close() {
        closed.set(true);
    }
}
