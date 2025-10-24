package de.bethibande.messaging.router;


import de.bethibande.messaging.locking.SpinningLock;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class MessageRouter {

    public static final long WILDCARD_VALUE = 0;

    private final AtomicLong subscriptionId = new AtomicLong();
    private final RouterNode root = new RouterNode(null, -1);

    private final List<RouteSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final Supplier<Queue<RouterNode>> queueSupplier;

    private final SpinningLock keyLock = new SpinningLock();
    private final AtomicLong keyCounter = new AtomicLong(0);
    private final Map<String, Long> keys = new WeakHashMap<>();

    public MessageRouter() {
        this(ArrayDeque::new);
    }

    public MessageRouter(final Supplier<Queue<RouterNode>> queueSupplier) {
        this.queueSupplier = queueSupplier;
        createKey("*"); // Ensure the wildcard is always present with id 0
    }

    protected long[] generateKey(final String[] key) {
        final long[] values = new long[key.length];

        final long ticket = this.keyLock.lockSpinning();
        try {
            for (int i = 0; i < key.length; i++) {
                final long value = this.keys.getOrDefault(key[i], this.keyCounter.getAndIncrement());
                values[i] = value;
                this.keys.put(key[i], value);
            }
        } finally {
            this.keyLock.unlock(ticket);
        }

        return values;
    }

    public PreComputedKey createKey(final String... key) {
        return new PreComputedKey(key, this.generateKey(key));
    }

    protected RouteSubscription createSubscription(final PreComputedKey key, final MessageConsumer consumer) {
        final long id = this.subscriptionId.getAndIncrement();
        final RouteSubscription subscription = new RouteSubscription(id, key, consumer);

        subscriptions.add(subscription);

        return subscription;
    }

    public List<RouteSubscription> getSubscriptions() {
        return subscriptions;
    }

    public RouteSubscription subscribe(final BiConsumer<PreComputedKey, Object> consumer, final PreComputedKey key) {
        final RouteSubscription subscription = this.createSubscription(
                key,
                (s, a, m) -> consumer.accept(s.getKey(), m)
        );

        this.addSubscriber(subscription);
        return subscription;
    }

    public RouteSubscription subscribe(final MessageConsumer consumer, final PreComputedKey key) {
        final RouteSubscription subscription = this.createSubscription(
                key,
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

    public void post(final PreComputedKey key, final Object message) {
        final long[] route = key.values();

        final Queue<RouterNode> stack = this.queueSupplier.get();
        stack.offer(root);

        while (!stack.isEmpty()) {
            final RouterNode current = stack.poll();

            current.post0(key, message);

            if (route.length == current.getRouterDepth() + 1) continue;

            final Map<Long, RouterNode> children = current.getChildren();

            final RouterNode wildcard = children.get(WILDCARD_VALUE);
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
