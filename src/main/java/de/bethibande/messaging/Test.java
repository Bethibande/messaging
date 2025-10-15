package de.bethibande.messaging;

import de.bethibande.messaging.concurrent.ExecutorPool;
import de.bethibande.messaging.concurrent.PooledExecutor;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Test {

    static void main() throws IOException  {
        final ExecutorPool pool = new ExecutorPool(32);

        final PooledExecutor executor = pool.getExecutor();
        executor.scheduleRepeating(_ -> System.out.println("Interval"), 1, TimeUnit.SECONDS);
        executor.schedule(() -> System.out.println("Later"), 5, TimeUnit.SECONDS);
        executor.queue(() -> System.out.println("Hello World!"));

        pool.shutdown();
    }

}
