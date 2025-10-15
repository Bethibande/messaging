package de.bethibande.messaging.net;

import de.bethibande.memory.Buffer;
import de.bethibande.memory.impl.DefaultBuffer;
import de.bethibande.memory.impl.ExpandingBuffer;
import de.bethibande.messaging.concurrent.PooledExecutor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class ChannelLoop {

    private static final long BUFFER_REGION_SIZE = 1L << 16;

    private final SocketChannel channel;
    private final PooledExecutor executor;
    private final SelectionKey key;

    private final ExpandingBuffer buffer;

    public ChannelLoop(final SocketChannel channel, final PooledExecutor executor) throws IOException {
        this.channel = channel;
        this.executor = executor;

        this.channel.configureBlocking(false);
        this.key = this.channel.register(this.executor.getSelector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE);

        this.buffer = new ExpandingBuffer(2, 16, executor.getAllocator());
    }

    protected void tryWrite(final Buffer buffer) {
        if (!channel.isOpen()) return;
        if (!key.isWritable()) {
            executor.schedule(() -> tryWrite(buffer), 1, TimeUnit.MICROSECONDS);
            return;
        }

        final long readable = buffer.readable();
        if (readable == 0) return;

        final int bufferCount = (int) (buffer.readable() / BUFFER_REGION_SIZE);
        final ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        for (int i = 0; i < bufferCount; i++) {
            final DefaultBuffer localBuffer = (DefaultBuffer) this.buffer.bufferAt(i * BUFFER_REGION_SIZE);
            buffers[i] = localBuffer.asNioBuffer();
        }
    }

    protected long tryRead() {
        if (buffer.writable() < BUFFER_REGION_SIZE) {
            buffer.expand();
        }

        final int bufferCount = (int) (buffer.capacity() / BUFFER_REGION_SIZE);
        final ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        for (int i = 0; i < bufferCount; i++) {
            final DefaultBuffer localBuffer = (DefaultBuffer) this.buffer.bufferAt(i * BUFFER_REGION_SIZE);
            buffers[i] = localBuffer.asNioBuffer();
        }

        buffers[0].position((int) this.buffer.readPosition());

        try {
            final long read = channel.read(buffers);
            this.buffer.writePosition(this.buffer.readPosition() + read);

            // TODO: Read frames

            this.buffer.compact();
            return read;
        } catch (final IOException e) {
            // TODO: Error handling
        }
        return -1;
    }

}
