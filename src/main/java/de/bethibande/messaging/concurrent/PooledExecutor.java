package de.bethibande.messaging.concurrent;

import de.bethibande.messaging.locking.SpinningLock;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class PooledExecutor {

    private final SpinningLock queueLock = new SpinningLock();
    private final TreeMap<Long, Queue<Task>> queues = new TreeMap<>(Long::compareTo);

    private final long taskBackoffNanos;

    private final AtomicBoolean active = new AtomicBoolean(true);
    private final Selector selector = Selector.open();

    public PooledExecutor(final long taskBackoffNanos) throws IOException {
        this.taskBackoffNanos = taskBackoffNanos;
    }

    public Selector getSelector() {
        return this.selector;
    }

    public void queue(final Runnable runnable) {
        this.queue(_ -> runnable.run());
    }

    public void schedule(final Runnable runnable, final long delay, final TimeUnit unit) {
        this.schedule(_ -> runnable.run(), delay, unit);
    }

    public void scheduleRepeating(final Task task, final long delay, final TimeUnit unit) {
        final Task repeatingTask = (exec) -> {
            try {
                task.run(exec);
            } finally {
                scheduleRepeating(task, delay, unit);
            }
        };

        schedule(repeatingTask, delay, unit);
    }

    public void schedule(final Task task, final long delay, final TimeUnit unit) {
        if (!this.active.get()) return;
        final long ticket = this.queueLock.lockSpinning();
        try {
            this.queues.computeIfAbsent(System.nanoTime() + unit.toNanos(delay), _ -> new ArrayDeque<>()).offer(task);
        } finally {
            this.queueLock.unlock(ticket);
        }
    }

    public void queue(final Task task) {
        schedule(task, 0, TimeUnit.NANOSECONDS);
    }

    public void shutdown() {
        this.active.set(false);
    }

    protected void loop() {
        while (this.active.get() || !queues.isEmpty()) {
            final long time = System.nanoTime();
            final Map.Entry<Long, Queue<Task>> entry = this.queues.floorEntry(time);
            if (entry == null) {
                LockSupport.parkNanos(this.taskBackoffNanos);
                continue;
            }

            final Queue<Task> queue = entry.getValue();
            final Task task = queue.poll();
            if (task == null || queue.isEmpty()) {
                final long ticket = this.queueLock.lockSpinning();
                try {
                    this.queues.remove(entry.getKey());
                } finally {
                    this.queueLock.unlock(ticket);
                }
                if (task == null) continue;
            }

            try {
                task.run(this);
            } catch (final Throwable th) {
                th.printStackTrace(); // TODO: Error handling
            }
        }
    }

}
