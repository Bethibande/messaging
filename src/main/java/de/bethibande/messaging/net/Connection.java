package de.bethibande.messaging.net;

import de.bethibande.memory.Buffer;
import de.bethibande.memory.impl.CompositeBuffer;
import de.bethibande.memory.impl.DefaultBuffer;
import de.bethibande.memory.impl.ExpandingBuffer;
import de.bethibande.memory.impl.JavaNioBuffer;
import de.bethibande.messaging.Test;
import de.bethibande.messaging.concurrent.PooledExecutor;
import de.bethibande.messaging.locking.SpinningLock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

public class Connection implements Selectable {

    protected static final long BUFFER_REGION_SIZE = 1L << 16;

    private final SocketChannel channel;
    private final PooledExecutor executor;
    private SelectionKey key;

    private final ExpandingBuffer readBuffer;

    private final SpinningLock lock = new SpinningLock();
    private final Queue<Buffer> writeQueue = new ArrayDeque<>(1);

    private long bytesReceived = 0;

    public Connection(final SocketChannel channel, final PooledExecutor executor) {
        this.channel = channel;
        this.executor = executor;
        this.readBuffer = new ExpandingBuffer(2, 16, executor.getAllocator());
    }

    public void register() throws IOException {
        this.channel.configureBlocking(false);
        this.key = this.channel.register(executor.getSelector(), SelectionKey.OP_READ);
        this.key.attach(this);
    }

    public void connect(final InetSocketAddress address) throws IOException {
        this.channel.connect(address);
        this.channel.finishConnect();

        this.channel.configureBlocking(false);
        this.key = this.channel.register(executor.getSelector(), SelectionKey.OP_READ);
        this.key.attach(this);
    }

    @Override
    public boolean onSelection() throws IOException {
        if (!this.channel.isOpen()) return false;

        if (this.key.isWritable()) {
            if (!this.writeQueue.isEmpty()) {
                return tryWrite();
            } else {
                final long ticket = this.lock.lockSpinning();
                try {
                    this.key.interestOps(SelectionKey.OP_READ);
                } finally {
                    this.lock.unlock(ticket);
                }
                return false;
            }
        }

        if (this.key.isReadable()) {
            return tryRead();
        }

        return false;
    }

    protected ByteBuffer[] toNioBuffers(final CompositeBuffer buffer, final long start, final long length) {
        final int startBufferIndex = (int) (start >> 16);
        final int endBufferIndex = (int) ((start + length) >> 16) - 1;
        final int bufferCount = endBufferIndex - startBufferIndex + 1;

        final ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        long offset = start;
        int index = 0;
        while (offset < start + length) {
            final Buffer buf = buffer.bufferAt(offset);
            buffers[index++] = ((JavaNioBuffer) buf).unwrap();

            offset += Math.min(length - (offset - start), BUFFER_REGION_SIZE);
        }
        buffers[0].position((int) (start & 0xFFFF));

        return buffers;
    }

    public void write(final Buffer buffer) {
        final long ticket = this.lock.lockSpinning();
        try {
            this.writeQueue.offer(buffer);
            this.key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        } finally {
            this.lock.unlock(ticket);
        }
    }

    protected boolean tryWrite() throws IOException {
        final long ticket = this.lock.lockSpinning();
        try {
            final Buffer buffer = this.writeQueue.poll();
            switch (buffer) {
                case null -> {
                    return false;
                }
                case CompositeBuffer composite -> {
                    final ByteBuffer[] nioBuffers = toNioBuffers(composite, buffer.readPosition(), buffer.readable());
                    this.channel.write(nioBuffers);

                    buffer.release();
                }
                case DefaultBuffer defaultBuffer -> {
                    final ByteBuffer nioBuffer = defaultBuffer.asNioBuffer();
                    nioBuffer.position((int) defaultBuffer.readPosition());
                    nioBuffer.limit((int) (defaultBuffer.readPosition() + defaultBuffer.readable()));

                    this.channel.write(nioBuffer);
                    defaultBuffer.release();
                }
                default -> {
                    buffer.release();
                    throw new IllegalStateException("Unknown buffer type: " + buffer.getClass().getName() + "; must be an instance of either CompositeBuffer or DefaultBuffer");
                }
            }

            if (this.writeQueue.isEmpty()) this.key.interestOps(SelectionKey.OP_READ);

            return !writeQueue.isEmpty();
        } finally {
            this.lock.unlock(ticket);
        }
    }

    protected boolean tryRead() throws IOException {
        if (this.readBuffer.writable() < BUFFER_REGION_SIZE * 2) {
            this.readBuffer.expand();
        }

        final ByteBuffer[] buffers = toNioBuffers(this.readBuffer, this.readBuffer.writePosition(), this.readBuffer.writable());
        final long read = this.channel.read(buffers);
        if (read == 0) return false;
        if (read < 0) {
            close();
            return false;
        }

        if (Test.EXPECTED_BYTES != 0) {
            this.bytesReceived += read;
            if (this.bytesReceived >= Test.EXPECTED_BYTES) {
                final long time = System.currentTimeMillis() - Test.START;
                System.out.println("Done reading in " + time + "ms | " + this.bytesReceived + " bytes received");
            }
        }

        this.readBuffer.writePosition(this.readBuffer.writePosition() + read);
        // TODO: Process data
        this.readBuffer.readPosition(this.readBuffer.readPosition() + read);

        this.readBuffer.compact(); // Discard all processed buffers from the composite buffer

        return true;
    }

    private void close() throws IOException {
        this.channel.close();
        this.key.cancel();
    }

}
