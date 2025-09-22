package de.bethibande.messaging.net.common.frame;

import io.netty.buffer.ByteBuf;

public class Frames implements FrameReader, FrameWriter {

    public static final byte ID_MESSAGE = 0x01;
    public static final byte ID_SUB_MESSAGE = 0x02;
    public static final byte ID_SUB = 0x03;
    public static final byte ID_SUB_ACK = 0x04;
    public static final byte ID_UNSUB = 0x05;
    public static final byte ID_UNSUB_ACK = 0x06;

    @Override
    public Frame read(final ByteBuf src) {
        final byte id = src.readByte();

        final Frame output = switch (id) { // Create a new frame
            case ID_MESSAGE -> new MessageFrame();
            case ID_SUB_MESSAGE -> new SubMessageFrame();
            case ID_SUB -> new SubscribeFrame();
            case ID_SUB_ACK -> new SubscribeAckFrame();
            case ID_UNSUB -> new UnSubscribeFrame();
            case ID_UNSUB_ACK -> new UnSubscribeAckFrame();
            default -> throw new IllegalArgumentException("Unknown frame id: " + id);
        };
        output.read(src, this); // Initialize new frame from buffer

        return output;
    }

    @Override
    public void write(final Frame frame, final ByteBuf dst) {
        final int mark = dst.writerIndex();

        dst.writeShort(0);
        dst.writeByte(switch (frame) {
            case MessageFrame _ -> ID_MESSAGE;
            case SubMessageFrame _ -> ID_SUB_MESSAGE;
            case SubscribeFrame _ -> ID_SUB;
            case SubscribeAckFrame _ -> ID_SUB_ACK;
            case UnSubscribeFrame _ -> ID_UNSUB;
            case UnSubscribeAckFrame _ -> ID_UNSUB_ACK;
        });
        frame.write(dst, this);

        dst.setShort(mark, dst.writerIndex() - mark - 2);
    }
}
