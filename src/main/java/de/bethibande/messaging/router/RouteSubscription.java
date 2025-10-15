package de.bethibande.messaging.router;

public class RouteSubscription {

    private final long id;
    private final String[] route;
    private RouterNode target;

    public RouteSubscription(final long id, final String[] route) {
        this.id = id;
        this.route = route;
    }

    public void remove() {
        this.target.removeSubscriber(this);
    }

    public long getId() {
        return id;
    }

    /*public Channel getChannel() {
        return channel;
    }*/

    public String[] getRoute() {
        return route;
    }

    public RouterNode getTarget() {
        return target;
    }

    /*public ChannelFuture post(final MessageFrame msg) {
        final SubMessageFrame frame = new SubMessageFrame();
        frame.setSubscriptionId(this.id);
        frame.setKey(msg.getKey());
        frame.setMessage(msg.getBody());

        return this.channel.writeAndFlush(frame);
    }*/

    public void setTarget(final RouterNode target) {
        this.target = target;
    }
}
