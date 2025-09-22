package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public final class MessageFrame implements Frame {

    private String[] key;
    private ByteBuf body;

    public MessageFrame() {
    }

    public MessageFrame(final String[] key, final ByteBuf body) {
        this.key = key;
        this.body = body;
    }

    public void setKey(final String[] key) {
        this.key = key;
    }

    public void setBody(final ByteBuf body) {
        this.body = body;
    }

    public String[] getKey() {
        return key;
    }

    public ByteBuf getBody() {
        return body;
    }

    @Override
    public void read(final ByteBuf src, final FrameReader reader) {
        this.key = FrameUtil.readKey(src);
        this.body = src.readBytes(src.readableBytes());
    }

    @Override
    public void write(final ByteBuf dst, final FrameWriter writer) {
        FrameUtil.writeKey(this.key, dst);
        // Write without modifying the reader index of the body buffer.
        // This is necessary to ensure we can copy the message to all subscribers as is.
        dst.writeBytes(this.body, 0, this.body.readableBytes());
    }
}
