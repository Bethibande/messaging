# Messaging
This is an implementation of a very simple MQTT like high-throughput, low-latency, in-memory message broker.
This router can deliver messages to subscribers with sub-microsecond latency at hundreds of millions of messages per second.
The primary factor limiting performance at this point is the JVMs ability to iterate over and invoke the subscribers of each node.
Performance may also drastically vary depending on the JVM used.

Each key is broken into individual nodes which are assembled into a tree structure by the router.
A message must be posted using a concrete key like `entities/user/ID` whereas a subscriber can subscribe to individual topics like `entities/user/ID`.
Subscribers may also use wildcards in their keys like `entities/user/*` or `entities/*/someValue`.

### Example
```java
void main() {
    final MessageRouter router = new MessageRouter();
    router.subscribe((subscription, actualRoute, message) -> {/* do something */}, "entities", "user");

    final String[] key = {"entities", "user", "12345"};
    router.post(key, "test");
}
```

## Benchmarks
### Throughput
Coming soon

### Latency
Coming soon