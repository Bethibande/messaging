package de.bethibande.messaging.net.client;

import de.bethibande.messaging.net.common.frame.SubMessageFrame;
import de.bethibande.messaging.net.common.frame.UnSubscribeFrame;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ClientSubscription {

    private final MessageClient client;
    private final Channel channel;
    private final long id;
    private final String[] path;

    private final List<Consumer<SubMessageFrame>> listeners = new ArrayList<>();

    public ClientSubscription(final MessageClient client, final Channel channel, final long id, final String[] path) {
        this.client = client;
        this.channel = channel;
        this.id = id;
        this.path = path;
    }

    public void addListener(final Consumer<SubMessageFrame> listener) {
        listeners.add(listener);
    }

    public void receive(final SubMessageFrame msg) {
        for (int i = 0; i < listeners.size(); i++) {
            listeners.get(i).accept(msg);
        }
        msg.getMessage().release();
    }

    public Promise<Void> remove() {
        // TODO: Timeout config
        final ClientUnsubscribeRequest request = client.createRequest(id -> new ClientUnsubscribeRequest(id, channel, 5_000, this.id));
        final UnSubscribeFrame frame = new UnSubscribeFrame();
        frame.setRequestId(request.getId());
        frame.setSubscriptionId(id);

        client.writeAndFlush(frame);
        return request.getFuture().addListener(_ -> client.unregisterSubscription(this));
    }

    public MessageClient getClient() {
        return client;
    }

    public long getId() {
        return id;
    }

    public String[] getPath() {
        return path;
    }
}
