package de.bethibande.messaging.router;

@FunctionalInterface
public interface MessageConsumer {

    void accept(final RouteSubscription subscription, final String[] route, final Object message);

}
