package de.bethibande.messaging.router;


import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class MessageRouter {

    private final AtomicLong subscriptionId = new AtomicLong();
    private final RouterNode root = new RouterNode(null, -1);

    private final List<RouteSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final Supplier<Queue<RouterNode>> queueSupplier;

    public MessageRouter() {
        this(ArrayDeque::new);
    }

    public MessageRouter(final Supplier<Queue<RouterNode>> queueSupplier) {
        this.queueSupplier = queueSupplier;
    }

    protected RouteSubscription createSubscription(final String[] route, final MessageConsumer consumer) {
        final long id = this.subscriptionId.getAndIncrement();
        final RouteSubscription subscription = new RouteSubscription(id, route) {
            @Override
            public void post(final String[] actualRoute, final Object message) {
                consumer.accept(this, actualRoute, message);
            }
        };

        subscriptions.add(subscription);

        return subscription;
    }

    public List<RouteSubscription> getSubscriptions() {
        return subscriptions;
    }

    public RouteSubscription subscribe(final BiConsumer<String[], Object> consumer, final String... route) {
        final RouteSubscription subscription = this.createSubscription(
                route,
                (s, a, m) -> consumer.accept(s.getRoute(), m)
        );

        this.addSubscriber(subscription);
        return subscription;
    }

    public RouteSubscription subscribe(final MessageConsumer consumer, final String... route) {
        final RouteSubscription subscription = this.createSubscription(
                route,
                consumer
        );

        this.addSubscriber(subscription);
        return subscription;
    }

    public void addSubscriber(final RouteSubscription subscriber) {
        root.addSubscriber(subscriber);
    }

    public void removeSubscriber(final RouteSubscription subscriber) {
        root.removeSubscriber(subscriber);
    }

    public void post(final String[] route, final Object message) {
        final Queue<RouterNode> stack = this.queueSupplier.get();
        stack.offer(root);

        while (!stack.isEmpty()) {
            final RouterNode current = stack.poll();

            current.post0(route, message);

            if (route.length == current.getRouterDepth() + 1) continue;

            final Map<String, RouterNode> children = current.getChildren();

            final RouterNode wildcard = children.get("*");
            if (wildcard != null) stack.offer(wildcard);

            final RouterNode directMatch = children.get(route[current.getRouterDepth() + 1]);
            if (directMatch != null) stack.offer(directMatch);
        }
    }

    public int countNodes() {
        int count = -1;
        final Queue<RouterNode> stack = new ArrayDeque<>();
        stack.add(root);

        while (!stack.isEmpty()) {
            final RouterNode node = stack.poll();
            count++;
            stack.addAll(node.getChildren().values());
        }

        return count;
    }

}
