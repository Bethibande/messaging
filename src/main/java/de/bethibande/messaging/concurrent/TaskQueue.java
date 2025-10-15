package de.bethibande.messaging.concurrent;

import de.bethibande.messaging.locking.SpinningLock;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class TaskQueue {

    private final SpinningLock scheduledLock = new SpinningLock();
    private final PriorityQueue<Task> scheduledQueue = new PriorityQueue<>(Comparator.comparing(Task::getTimestamp));
    private final SpinningLock lock = new SpinningLock();
    private final Queue<Runnable> queue = new ArrayDeque<>();

    public void queue(final Runnable runnable) {
        final long ticket = this.lock.lockSpinning();
        try {
            this.queue.offer(runnable);
        } finally {
            this.lock.unlock(ticket);
        }
    }

    public void schedule(final Task task) {
        final long ticket = this.scheduledLock.lockSpinning();
        try {
            this.scheduledQueue.offer(task);
        } finally {
            this.scheduledLock.unlock(ticket);
        }
    }

    public boolean isEmpty() {
        return this.queue.isEmpty() && this.scheduledQueue.isEmpty();
    }

    public Runnable poll() {
        final long ticket = this.lock.lockSpinning();
        try {
            final Runnable runnable = this.queue.poll();
            if (runnable != null) return runnable;
        } finally {
            this.lock.unlock(ticket);
        }

        final long scheduledTicket = this.scheduledLock.lockSpinning();
        try {
            final Task task = this.scheduledQueue.poll();
            if (task == null) return null;
            if (!task.canExecute(System.nanoTime())) {
                this.scheduledQueue.offer(task);
                return null;
            }

            return task.getRunnable();
        } finally {
            this.scheduledLock.unlock(scheduledTicket);
        }
    }

}
