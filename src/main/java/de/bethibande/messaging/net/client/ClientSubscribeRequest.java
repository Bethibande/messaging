package de.bethibande.messaging.net.client;

import de.bethibande.messaging.net.common.AbstractRequest;
import de.bethibande.messaging.net.common.frame.Frame;
import de.bethibande.messaging.net.common.frame.SubscribeAckFrame;
import io.netty.channel.Channel;

public class ClientSubscribeRequest extends AbstractRequest<ClientSubscription> {

    private final long subscriptionId;
    private final String[] path;
    private final MessageClient client;

    public ClientSubscribeRequest(final long id,
                                  final Channel channel,
                                  final long timeoutMillis,
                                  final long subscriptionId,
                                  final String[] path,
                                  final MessageClient client) {
        super(id, channel, timeoutMillis);

        this.subscriptionId = subscriptionId;
        this.path = path;
        this.client = client;
    }

    public long getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    protected ClientSubscription handle(final Frame response) {
        if (response instanceof SubscribeAckFrame ack) {
            if (ack.getSubscriptionId() != this.subscriptionId) {
                throw new IllegalStateException("Unexpected subscription id: " + ack.getSubscriptionId());
            }

            final ClientSubscription sub = new ClientSubscription(client, channel, subscriptionId, path);
            client.registerSubscription(sub);
            return sub;
        }
        throw new IllegalStateException("Unexpected response: " + response);
    }
}
