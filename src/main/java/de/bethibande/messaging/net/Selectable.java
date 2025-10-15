package de.bethibande.messaging.net;

import java.io.IOException;

public interface Selectable {

    /**
     * Handles a selection event triggered by a NIO selector.
     * This method is responsible for processing the event based on the state
     * of the associated channel or key, such as accepting a connection or
     * processing read operations.
     * <br><br>
     * This method must always perform only a single operation such as read, write or accept to ensure fair scheduling.
     * For example, when reading data, call {@code SocketChannel.read} exactly once and then return true if bytes were read and false otherwise.
     * The {@link de.bethibande.messaging.concurrent.PooledExecutor} will call this method in a loop until it returns false.
     *
     * @return true if the selection resulted in a meaningful state change or activity,
     *         false otherwise.
     * @throws IOException if an I/O error occurs while handling the selection.
     */
    boolean onSelection() throws IOException;

}
