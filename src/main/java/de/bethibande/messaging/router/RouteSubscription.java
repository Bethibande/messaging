package de.bethibande.messaging.router;

public abstract class RouteSubscription {

    private final long id;
    private final String[] route;
    private RouterNode target;

    public RouteSubscription(final long id, final String[] route) {
        this.id = id;
        this.route = route;
    }

    public long getId() {
        return id;
    }

    public String[] getRoute() {
        return route;
    }

    public RouterNode getTarget() {
        return target;
    }

    public abstract void post(final String[] route, final Object message);

    public void setTarget(final RouterNode target) {
        this.target = target;
    }
}
