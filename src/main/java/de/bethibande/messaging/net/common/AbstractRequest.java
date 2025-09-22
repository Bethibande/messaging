package de.bethibande.messaging.net.common;

import de.bethibande.messaging.net.common.frame.Frame;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractRequest<T> {

    protected final long id;
    protected final Channel channel;
    private final ScheduledFuture<?> timeout;
    protected final Promise<T> future;

    public AbstractRequest(final long id, final Channel channel, final long timeoutMillis) {
        this.id = id;
        this.channel = channel;
        this.timeout = channel.eventLoop().schedule(
                this::onTimeout,
                timeoutMillis,
                TimeUnit.MILLISECONDS
        );
        this.future = channel.eventLoop().newPromise();
    }

    public long getId() {
        return id;
    }

    public Promise<T> getFuture() {
        return future;
    }

    protected abstract T handle(final Frame response);

    public void receive(final Frame frame) {
        try {
            timeout.cancel(false);
            future.setSuccess(handle(frame));
        } catch (final Throwable th) {
            future.setFailure(th);
        }
    }

    protected void onTimeout() {
        future.setFailure(new TimeoutException("Request timed out"));
    }

}
