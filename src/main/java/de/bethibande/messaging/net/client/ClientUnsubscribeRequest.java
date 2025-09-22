package de.bethibande.messaging.net.client;

import de.bethibande.messaging.net.common.AbstractRequest;
import de.bethibande.messaging.net.common.frame.Frame;
import de.bethibande.messaging.net.common.frame.UnSubscribeAckFrame;
import io.netty.channel.Channel;

public class ClientUnsubscribeRequest extends AbstractRequest<Void> {

    private final long subscriptionId;

    public ClientUnsubscribeRequest(final long id,
                                    final Channel channel,
                                    final long timeoutMillis,
                                    final long subscriptionId) {
        super(id, channel, timeoutMillis);
        this.subscriptionId = subscriptionId;
    }

    @Override
    protected Void handle(final Frame response) {
        if (response instanceof UnSubscribeAckFrame ack) {
            if (ack.getSubscriptionId() != this.subscriptionId) {
                throw new IllegalStateException("Unexpected subscription id: " + ack.getSubscriptionId());
            }

            return null;
        }
        throw new IllegalStateException("Unexpected response: " + response);
    }
}
