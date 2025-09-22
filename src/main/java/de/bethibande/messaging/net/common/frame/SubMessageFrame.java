package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public final class SubMessageFrame implements Frame {

    private long subscriptionId;
    private String[] key;
    private ByteBuf message;

    @Override
    public void read(final ByteBuf src, final FrameReader reader) {
        this.subscriptionId = src.readLong();
        this.key = FrameUtil.readKey(src);
        this.message = src.readBytes(src.readShort());
    }

    @Override
    public void write(final ByteBuf dst, final FrameWriter writer) {
        dst.writeLong(this.subscriptionId);
        FrameUtil.writeKey(this.key, dst);
        dst.writeShort(this.message.readableBytes());
        dst.writeBytes(this.message, 0, this.message.readableBytes()); // Copy instead of modifying the reader index of the message buffer.
    }

    public void setSubscriptionId(final long subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public void setKey(final String[] key) {
        this.key = key;
    }

    public void setMessage(final ByteBuf message) {
        this.message = message;
    }

    public long getSubscriptionId() {
        return subscriptionId;
    }

    public String[] getKey() {
        return key;
    }

    public ByteBuf getMessage() {
        return message;
    }
}
