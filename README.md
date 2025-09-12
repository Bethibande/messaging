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
These benchmarks are run on an AMD Ryzen 9 5950x with 64 GB of RAM.
Before the benchmark, a router with n subscribers is initialized. The subscribers are randomly generated using a hard-coded seed.
Each subscriber is subscribed to a random topic. Each generated topic is 2 or 3 tokens long. The 2nd and 3rd tokens
have a chance of being wildcards.
The benchmark then posts messages using a static key to ensure we don't perform any heap allocations during the benchmark.
With this setup, we ensure each posted message triggers a large portion of all registered subscribers.
There is no optimization for repeated posts to the same key as such we can assume that posting messages to any available topic
will be roughly the same.

All benchmarks are run on GraalVM 24.0.1. Depending on the benchmark type different GC settings are used.
See the description of each section for more details. GC settings can greatly affect throughput and latency
even though the benchmarks perform little to no heap allocations after warmup.

The blow table indicates the number of subscribers along with the number of total generated nodes
and how many subscribers are hit for each message posted in our benchmark.

| subscribers | nodes | hits  |
|-------------|-------|-------|
| 10          | 16    | 1     |
| 100         | 67    | 15    |
| 1k          | 463   | 155   |
| 10k         | 3385  | 1624  |
| 100k        | 12121 | 16048 |

### Throughput
The below tests are all run using the default GC provided by GraalVM. Using ZGC yields ~50% less throughput.

| subscribers | messages per second |
|-------------|---------------------|
| 10          | 575.7 million       |
| 100         | 289.6 million       |
| 1k          | 47.3 million        |
| 10k         | 3.6 million         |
| 100k        | 392 thousand        |


### Latency
The below tests are all run using ZGC as it yields much better and stable latencies.

| subscribers | p0.00   | p0.50    | p0.90    | p0.95    | p0.99    | p0.999   | p0.9999   | p1.00    |
|-------------|---------|----------|----------|----------|----------|----------|-----------|----------|
| 10          | ?       | 100 ns   | 100 ns   | 100 ns   | 100 ns   | 100 ns   | 4296 ns   | 21.3 ms  |
| 100         | ?       | 200 ns   | 200 ns   | 200 ns   | 200 ns   | 200 ns   | 6296 ns   | 27.7 ms  |
| 1k          | 300 ns  | 800 ns   | 900 ns   | 900 ns   | 900 ns   | 1000 ns  | 28075 ns  | 199.9 ms |
| 10k         | 5800 ns | 12800 ns | 12992 ns | 12992 ns | 14000 ns | 24800 ns | 129152 ns | 35.3 ms  |

For peak latencies it's recommended to not run the benchmark on all available CPUs. The following benchmarks are run on 16 instead of 32 threads.

| subscribers | p0.00   | p0.50   | p0.90   | p0.95   | p0.99   | p0.999   | p0.9999  | p1.00      |
|-------------|---------|---------|---------|---------|---------|----------|----------|------------|
| 10          | ?       | 100 ns  | 100 ns  | 100 ns  | 100 ns  | 100 ns   | 2900 ns  | 131.6 µs   |
| 100         | ?       | 100 ns  | 100 ns  | 100 ns  | 100 ns  | 200 ns   | 3100 ns  | 190.0 µs   |
| 1k          | 300 ns  | 400 ns  | 400 ns  | 500 ns  | 700 ns  | 800 ns   | 6496 ns  | 217.1 µs   |
| 10k         | 3800 ns | 4696 ns | 4800 ns | 4800 ns | 8496 ns | 12896 ns | 30784 ns | 269.3.1 µs |

### Conclusion
The key takeaway from these benchmarks is that the router is able to deliver messages to subscribers with sub-microsecond latency at hundreds of millions of messages per second under ideal conditions.
However, an increased routing table size, including larger subscriber counts, will diminish the throughput of the router.
One key reason for this is the likely contention around L1 and L2 CPU caches. That's why latency improves a lot when running only on thread per core instead of one per processor.
At the same time invoking the listeners is quite expensive since they are interface method invocations. Though this likely depends on the JVM used.
Based on some other benchmarks I've run, I think it is safe to say, that subscriber count usually matters more than node count.
The routing table internally is a tree that makes heavy use of hash map lookups, as such node count even per parent node does not matter much — not nearly as much as listener count.