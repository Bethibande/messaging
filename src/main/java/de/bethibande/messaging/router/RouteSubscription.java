package de.bethibande.messaging.router;

public final class RouteSubscription {

    private final long id;
    private final PreComputedKey key;
    private final MessageConsumer consumer;
    private RouterNode target;

    public RouteSubscription(final long id, final PreComputedKey key, final MessageConsumer consumer) {
        this.id = id;
        this.key = key;
        this.consumer = consumer;
    }

    public long getId() {
        return id;
    }

    public PreComputedKey getKey() {
        return key;
    }

    public RouterNode getTarget() {
        return target;
    }

    public void post(final PreComputedKey key, final Object message) {
        this.consumer.accept(this, key, message);
    }

    public void setTarget(final RouterNode target) {
        this.target = target;
    }
}
