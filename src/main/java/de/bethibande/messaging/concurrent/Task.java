package de.bethibande.messaging.concurrent;

public class Task {

    private final long timestamp;
    private final Runnable runnable;

    public Task(final long timestamp, final Runnable runnable) {
        this.timestamp = timestamp;
        this.runnable = runnable;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public boolean canExecute(final long timestamp) {
        return this.timestamp <= timestamp;
    }

    public Runnable getRunnable() {
        return runnable;
    }
}
