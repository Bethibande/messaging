package de.bethibande.messaging.router;


import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Supplier;

public class MessageRouter {

    private final RouterNode root = new RouterNode(null, -1);

    private final Supplier<Queue<RouterNode>> queueSupplier;

    public MessageRouter() {
        this(ArrayDeque::new);
    }

    public MessageRouter(final Supplier<Queue<RouterNode>> queueSupplier) {
        this.queueSupplier = queueSupplier;
    }

    public void addSubscriber(final RouteSubscription subscriber) {
        root.addSubscriber(subscriber);
    }

    public void removeSubscriber(final RouteSubscription subscriber) {
        root.removeSubscriber(subscriber);
    }

    /*public void post(final MessageFrame frame) {
        final Queue<RouterNode> stack = this.queueSupplier.get();
        stack.offer(root);

        final String[] route = frame.getKey();

        while (!stack.isEmpty()) {
            final RouterNode current = stack.poll();

            current.post0(frame);

            if (route.length == current.getRouterDepth() + 1) continue;

            final Map<String, RouterNode> children = current.getChildren();

            final RouterNode wildcard = children.get("*");
            if (wildcard != null) stack.offer(wildcard);

            final RouterNode directMatch = children.get(route[current.getRouterDepth() + 1]);
            if (directMatch != null) stack.offer(directMatch);
        }
        frame.getBody().release();
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
    }*/

}
