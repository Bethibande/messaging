package de.bethibande.messaging.locking;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class SpinningLock {

    private final AtomicLong ticketCounter = new AtomicLong();
    private final AtomicLong owner = new AtomicLong(-1);

    public void unlock(final long ticket) {
        owner.compareAndSet(ticket, -1);
    }

    public boolean tryLock(final long ticket) {
        return owner.compareAndSet(-1, ticket);
    }

    public long lockSpinning() {
        final long ticket = this.ticketCounter.incrementAndGet();
        while (!tryLock(ticket)) {
            LockSupport.parkNanos(100);
        }
        return ticket;
    }

}
