package de.bethibande.messaging;

import de.bethibande.messaging.router.MessageRouter;
import de.bethibande.messaging.router.PreComputedKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;

public class Test {

    private static final List<String> CATEGORIES = List.of("entities", "logs");
    private static final List<String> ENTITIES = List.of("User", "Book", "Car", "Student", "Teacher", "Building");

    private static int COUNTER;

    public static void randomizeRouter(final MessageRouter router,
                                       final int subscribers,
                                       final BiConsumer<PreComputedKey, Object> consumer) {
        final Random random = new Random(234543654);
        for (int i = 0; i < subscribers; i++) {
            final List<String> strings = new ArrayList<>();
            strings.add(CATEGORIES.get(random.nextInt(CATEGORIES.size())));
            if (random.nextBoolean()) {
                strings.add(ENTITIES.get(random.nextInt(ENTITIES.size())));
            } else {
                strings.add("*");
            }
            if (random.nextBoolean()) {
                if (random.nextInt(10) >= 1) {
                    strings.add(String.valueOf(random.nextInt(1000)));
                } else {
                    strings.add("*");
                }
            }
            final PreComputedKey key = router.createKey(strings.toArray(String[]::new));
            router.subscribe(consumer, key);
        }
    }

    private static void accept(final PreComputedKey key, final Object message) {
        COUNTER++;
        System.out.println("Msg: " + Arrays.toString(key.key()) + ": " + message);
    }

    public static void main(String[] args) {
        final MessageRouter router = new MessageRouter();
//        router.subscribe(Test::accept, "entities", "*");
//        router.subscribe(Test::accept, "entities", "User", "*");
//        router.subscribe(Test::accept, "entities", "User", "abc");
//        router.subscribe(Test::accept, "entities", "User", "def");
//        router.subscribe(Test::accept, "entities", "User", "1500");
//        router.subscribe(Test::accept, "logs", "User", "1500");
        randomizeRouter(router, 10, Test::accept);

        final PreComputedKey key = router.createKey("entities", "User", "1500");
        router.post(key, "test");

        System.out.println("Nodes: " + router.countNodes() + " | Hits per msg: " + COUNTER);
    }

}
