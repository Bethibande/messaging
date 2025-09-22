package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public final class SubscribeFrame implements Frame, RequestFrame {

    private long requestId;
    private long subscriptionId;
    private String[] path;

    @Override
    public long getRequestId() {
        return requestId;
    }

    public long getSubscriptionId() {
        return subscriptionId;
    }

    public String[] getPath() {
        return path;
    }

    @Override
    public void setRequestId(final long requestId) {
        this.requestId = requestId;
    }

    public void setSubscriptionId(final long subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public void setPath(final String[] path) {
        this.path = path;
    }

    @Override
    public void read(final ByteBuf src, final FrameReader reader) {
        this.requestId = src.readLong();
        this.subscriptionId = src.readLong();
        this.path = FrameUtil.readKey(src);
    }

    @Override
    public void write(final ByteBuf dst, final FrameWriter writer) {
        dst.writeLong(this.requestId);
        dst.writeLong(this.subscriptionId);
        FrameUtil.writeKey(this.path, dst);
    }
}
