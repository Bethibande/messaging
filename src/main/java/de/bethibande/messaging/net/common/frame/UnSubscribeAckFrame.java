package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public final class UnSubscribeAckFrame implements Frame, RequestFrame {

    private long requestId;
    private long subscriptionId;

    @Override
    public long getRequestId() {
        return requestId;
    }

    public long getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    public void setRequestId(final long requestId) {
        this.requestId = requestId;
    }

    public void setSubscriptionId(final long subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    @Override
    public void read(final ByteBuf src, final FrameReader reader) {
        this.requestId = src.readLong();
        this.subscriptionId = src.readLong();
    }

    @Override
    public void write(final ByteBuf dst, final FrameWriter writer) {
        dst.writeLong(this.requestId);
        dst.writeLong(this.subscriptionId);
    }
}