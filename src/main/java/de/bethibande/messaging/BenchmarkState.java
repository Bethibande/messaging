package de.bethibande.messaging;

import de.bethibande.messaging.router.MessageRouter;
import de.bethibande.messaging.router.RouterNode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayDeque;
import java.util.Queue;

@State(Scope.Benchmark)
public class BenchmarkState {

    public final ThreadLocal<Queue<RouterNode>> queues = ThreadLocal.withInitial(ArrayDeque::new);
    public final MessageRouter router = new MessageRouter(queues::get);

    @Setup
    public void init(final Blackhole blackhole) {
        Test.randomizeRouter(this.router, 1000, (r, m) -> blackhole.consume(m));
    }

}
