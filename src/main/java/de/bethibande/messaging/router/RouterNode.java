package de.bethibande.messaging.router;

import de.bethibande.messaging.locking.SpinningLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RouterNode {

    private final RouterNode parent;
    private final String token;
    private final int routerDepth;

    private final SpinningLock childrenLock = new SpinningLock();
    private final Map<String, RouterNode> children = new HashMap<>(0);

    private final SpinningLock subscribersLock = new SpinningLock();
    private final List<RouteSubscription> subscribers = new ArrayList<>(0);

    public RouterNode(final RouterNode parent, final int routerDepth) {
        this(parent, "*", routerDepth);
    }

    public RouterNode(final RouterNode parent, final String token, final int routerDepth) {
        this.parent = parent;
        this.token = token;
        this.routerDepth = routerDepth;
    }

    public void addSubscriber(final RouteSubscription subscriber) {
        final String[] route = subscriber.getRoute();

        if (route.length == routerDepth + 1) {
            final long ticket = this.subscribersLock.lockSpinning();
            try {
                subscribers.add(subscriber);
                subscriber.setTarget(this);
            } finally {
                this.subscribersLock.unlock(ticket);
            }
        } else {
            final String nextKey = route[routerDepth + 1];
            final long ticket = this.childrenLock.lockSpinning();
            try {
                final RouterNode node = children.computeIfAbsent(
                        nextKey,
                        k -> new RouterNode(this, k, routerDepth + 1)
                );
                node.addSubscriber(subscriber);
            } finally {
                childrenLock.unlock(ticket);
            }
        }
    }

    protected void removeIfEmpty(final RouterNode child) {
        final long ticket = this.childrenLock.lockSpinning();
        try {
            if (child.subscribers.isEmpty() && child.children.isEmpty()) {
                this.children.remove(child.token);
                if (this.parent != null) this.parent.removeIfEmpty(this);
            }
        } finally {
            this.childrenLock.unlock(ticket);
        }
    }

    public void removeSubscriber(final RouteSubscription subscriber) {
        final String[] route = subscriber.getRoute();

        if (route.length == this.routerDepth + 1) {
            final long ticket = this.subscribersLock.lockSpinning();
            try {
                this.subscribers.remove(subscriber);
                if (this.parent != null) this.parent.removeIfEmpty(this);
            } finally {
                this.subscribersLock.unlock(ticket);
            }
        } else {
            final String nextKey = route[this.routerDepth + 1];
            final RouterNode node = this.children.get(nextKey);
            if (node != null) node.removeSubscriber(subscriber);
        }
    }

    public int getRouterDepth() {
        return routerDepth;
    }

    public Map<String, RouterNode> getChildren() {
        return this.children;
    }

    protected void post0(final String[] route, final Object message) {
        final int subscribers = this.subscribers.size();
        for (int i = 0; i < subscribers; i++) {
            this.subscribers.get(i).post(route, message);
        }
    }

}
